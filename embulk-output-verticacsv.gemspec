Gem::Specification.new do |spec|
  spec.name          = "embulk-output-verticacsv"
  spec.version       = "0.8.0"
  spec.authors       = ["NAMSU HEO"]
  spec.email         = ["nsheo@ntels.com"]
  spec.summary       = "Vertica output plugin for Embulk edited by nsheo"
  spec.description   = "Dump records to vertica"
  spec.homepage      = "https://github.com/nsheo/embulk-output-vertica"
  spec.license       = "MIT"

  spec.files         = `git ls-files -z`.split("\x0")
  spec.executables   = spec.files.grep(%r{^bin/}) { |f| File.basename(f) }
  spec.test_files    = spec.files.grep(%r{^(test|spec|features)/})
  spec.require_paths = ["lib"]

  spec.add_dependency "vertica", "~> 1.0.0"
  spec.add_dependency "time_with_zone", "~> 0.3"

  spec.add_development_dependency "bundler", "~> 1.7"
  spec.add_development_dependency "rake", "~> 10.0"
end
