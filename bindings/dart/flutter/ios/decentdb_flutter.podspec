Pod::Spec.new do |s|
  s.name             = 'decentdb_flutter'
  s.version          = '2.7.0'
  s.summary          = 'Flutter mobile integration helpers for DecentDB.'
  s.description      = 'Provides Flutter registration and native artifact wiring for the DecentDB Dart FFI package.'
  s.homepage         = 'https://github.com/sphildreth/decentdb'
  s.license          = { :type => 'MIT' }
  s.author           = { 'DecentDB Contributors' => 'maintainers@decentdb.dev' }
  s.source           = { :path => '.' }
  s.source_files     = 'Classes/**/*'
  s.dependency 'Flutter'
  s.platform = :ios, '15.0'
  s.static_framework = true
  s.swift_version = '5.0'
  s.vendored_frameworks = 'Frameworks/decentdb.xcframework'
  s.pod_target_xcconfig = {
    'DEFINES_MODULE' => 'YES'
  }
end
