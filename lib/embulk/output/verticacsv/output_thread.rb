require 'zlib'
require 'timeout'

module Embulk
  module Output
    class VerticaCSV < OutputPlugin
      class CommitError < ::StandardError; end
      class TimeoutError < ::Timeout::Error; end
      class DequeueTimeoutError < TimeoutError; end
      class FinishTimeoutError < TimeoutError; end
      class WriteTimeoutError < TimeoutError; end

      $embulk_output_vertica_thread_dumped = false

      class OutputThreadPool
        def initialize(task, schema, size)
          @task = task
          @size = size
          @schema = schema
          @converters = ValueConverterFactory.create_converters(schema, task['default_timezone'], task['column_options'])
          @output_threads = size.times.map { OutputThread.new(task) }
          @current_index = 0
        end

        def enqueue(page)
          csv_data = []
          #Embulk.logger.debug { "embulk-output-verticacsv: Check timeformat #{@task['column_options'][@task['load_time_col']]['timestamp_format']}" }
          current_time = Time.now.strftime(@task['column_options'][@task['load_time_col']]['timestamp_format'])
          page.each do |record|
            csv_data << to_csv(record)
            unless @task['load_time_col'].nil? 
              csv_data << @task['delimiter_str'] << current_time
            end
          end
          #Embulk.logger.debug { "embulk-output-verticacsv: Check data 2 #{csv_data}" }
          @mutex.synchronize do
            @output_threads[@current_index].enqueue(csv_data)
            @current_index = (@current_index + 1) % @size
          end
        end

        def start
          @mutex = Mutex.new
          @size.times.map {|i| @output_threads[i].start }
        end

        def commit
          Embulk.logger.debug "embulk-output-verticacsv: commit"
          task_reports = @mutex.synchronize do
            @size.times.map {|i| @output_threads[i].commit }
          end
          unless task_reports.all? {|task_report| task_report['success'] }
            raise CommitError, "some of output_threads failed to commit"
          end
          task_reports
        end

        def to_csv(record)
          Embulk.logger.debug { "embulk-output-verticacsv: check record #{record}" }
          if @task['csv_payload']
            record.first
          else
            record * @task['delimiter_str']
          end
        end
      end

      class OutputThread
        def initialize(task)
          @task = task
          @queue = SizedQueue.new(1)
          @num_input_rows = 0
          @num_output_rows = 0
          @num_rejected_rows = 0
          @outer_thread = Thread.current
          @thread_active = false
          @progress_log_timer = Time.now
          @previous_num_input_rows = 0
        end

        def thread_dump
          unless $embulk_output_vertica_thread_dumped
            $embulk_output_vertica_thread_dumped = true
            Embulk.logger.debug "embulk-output-verticacsv: kill -3 #{$$} (Thread dump)"
            begin
              Process.kill :QUIT, $$
            rescue SignalException
            ensure
              sleep 1
            end
          end
        end

        def enqueue(csv_data)
          if @thread_active and @thread.alive?
            Embulk.logger.trace { "embulk-output-verticacsv: enqueue" }
            @queue.push(csv_data)
          else
            Embulk.logger.info { "embulk-output-verticacsv: thread is dead, but still trying to enqueue" }
            thread_dump
            raise RuntimeError, "embulk-output-verticacsv: thread is died, but still trying to enqueue"
          end
        end

        # @return [Array] dequeued csv_data
        # @return [String] 'finish' is dequeued to finish
        def dequeue
          csv_data = nil
          Embulk.logger.trace { "embulk-output-verticacsv: @queue.pop with dequeue_timeout:#{@task['dequeue_timeout']}" }
          Timeout.timeout(@task['dequeue_timeout'], DequeueTimeoutError) { csv_data = @queue.pop }
          Embulk.logger.trace { "embulk-output-verticacsv: dequeued" }
          Embulk.logger.debug { "embulk-output-verticacsv: dequeued finish" } if csv_data == 'finish'
          csv_data
        end

        def copy(jv, sql, &block)
          Embulk.logger.debug "embulk-output-verticacsv: copy, waiting a first message"

          num_output_rows = 0; rejected_row_nums = []; last_record = nil

          csv_data = dequeue
          return [num_output_rows, rejected_row_nums, last_record] if csv_data == 'finish'

          Embulk.logger.debug "embulk-output-verticacsv: #{sql}"

          num_output_rows, rejected_row_nums = jv.copy(sql) do |stdin, stream|
            @write_proc.call(stdin, csv_data) {|record| last_record = record }

            while true
              csv_data = dequeue
              break if csv_data == 'finish'
              @write_proc.call(stdin, csv_data) {|record| last_record = record }
            end
          end

          @num_output_rows += num_output_rows
          @num_rejected_rows += rejected_row_nums.size
          Embulk.logger.info { "embulk-output-verticacsv: COMMIT!" }
          jv.commit
          Embulk.logger.debug { "embulk-output-verticacsv: COMMITTED!" }

          if rejected_row_nums.size > 0
            Embulk.logger.debug { "embulk-output-verticacsv: rejected_row_nums: #{rejected_row_nums}" }
          end

          [num_output_rows, rejected_row_nums, last_record]
        end
		

        def run
          Embulk.logger.debug { "embulk-output-verticacsv: thread started" }
          begin
            jv = VerticaCSV.connect(@task)
            begin
              num_output_rows, rejected_row_nums, last_record = copy(jv, copy_sql)
              Embulk.logger.debug { "embulk-output-verticacsv: thread finished" }
            rescue java.sql.SQLDataException => e
              if @task['reject_on_materialized_type_error'] and e.message =~ /Rejected by user-defined parser/
                Embulk.logger.warn "embulk-output-verticacsv: ROLLBACK! some of column types and values types do not fit #{rejected_row_nums}"
              else
                Embulk.logger.warn "embulk-output-verticacsv: ROLLBACK! #{rejected_row_nums}"
              end
              Embulk.logger.info { "embulk-output-verticacsv: last_record: #{last_record}" }
              rollback(jv)
              raise e
            rescue => e
              Embulk.logger.warn "embulk-output-verticacsv: ROLLBACK! #{e.class} #{e.message} #{e.backtrace.join("\n  ")}"
              rollback(jv)
              raise e
            end
          ensure
            close(jv)
          end
        rescue TimeoutError => e
          Embulk.logger.error "embulk-output-verticacsv: UNKNOWN TIMEOUT!! #{e.class}"
          @thread_active = false # not to be enqueued any more
          dequeue_all
          thread_dump
          exit(1)
        rescue Exception => e
          Embulk.logger.error "embulk-output-verticacsv: UNKNOWN ERROR! #{e.class} #{e.message} #{e.backtrace.join("\n  ")}"
          @thread_active = false # not to be enqueued any more
          dequeue_all
          @outer_thread.raise e
        end

        def dequeue_all
          Embulk.logger.debug "embulk-output-verticacsv: dequeue all"
          while @queue.size > 0
            @queue.pop # dequeue all because some might be still trying @queue.push and get blocked, need to release
          end
        end

        def close(jv)
          begin
            jv.close
          rescue java.sql.SQLException => e # The connection is closed
            Embulk.logger.debug "embulk-output-verticacsv: #{e.class} #{e.message}"
          end
        end

        def rollback(jv)
          begin
            jv.rollback
          rescue java.sql.SQLException => e # The connection is closed
            Embulk.logger.debug "embulk-output-verticacsv: #{e.class} #{e.message}"
          end
        end

        def start
          @thread = Thread.new(&method(:run))
          @thread_active = true
        end

        def commit
          Embulk.logger.debug "embulk-output-verticacsv: output_thread commit"
          @thread_active = false
          success = true
          if @thread.alive?
            Embulk.logger.debug { "embulk-output-verticacsv: push finish with finish_timeout:#{@task['finish_timeout']}" }
            @queue.push('finish')
            Thread.pass
            @thread.join(@task['finish_timeout'])
            if @thread.alive?
              @thread.kill
              Embulk.logger.error "embulk-output-verticacsv: finish_timeout #{@task['finish_timeout']}sec exceeded, thread is killed forcely"
              success = false
            end
          else
            Embulk.logger.error "embulk-output-verticacsv: thread died accidently"
            success = false
          end

          task_report = {
            'num_input_rows' => @num_input_rows,
            'num_output_rows' => @num_output_rows,
            'num_rejected_rows' => @num_rejected_rows,
            'success' => success
          }
        end

        # private

        def copy_sql
          @copy_sql ||= "COPY #{quoted_schema}.#{quoted_table} (#{column_info}) FROM STDIN#{compress}#{delimiter}#{copy_mode}#{abort_on_error} NO COMMIT"
        end

        def quoted_schema
          ::Vertica.quote_identifier(@task['schema'])
        end

        def quoted_table
          ::Vertica.quote_identifier(@task['table'])
        end

        def column_info
          sql_schema = @task['column_options'].keys
          sql_schema * ', '
        end

        def compress
          " #{@task['compress']}"
        end

        def copy_mode
          " #{@task['copy_mode']}"
        end

        def delimiter
          " DELIMITER '#{@task['delimiter_str']}' RECORD TERMINATOR E'\n' ENFORCELENGTH "
        end

        def abort_on_error
          @task['abort_on_error'] ? ' ABORT ON ERROR' : ''
        end

        def reject_on_materialized_type_error
          @task['reject_on_materialized_type_error'] ? 'reject_on_materialized_type_error=true' : ''
        end
      end
    end
  end
end
