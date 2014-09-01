#!/usr/bin/env rake
require 'bundler/setup'
require 'bundler/gem_tasks'
require 'rake/testtask'

Rake::TestTask.new do |t|
  t.test_files = FileList['test/test_*.rb']
end

Rake::TestTask.new(:'test-unit') do |t|
  t.test_files = FileList[
    'test/test_mongoriver.rb',
    'test/test_toku.rb',
    'test/test_persistent_tailers.rb']
end

Rake::TestTask.new(:'test-connected') do |t|
  t.test_files = FileList['test/test_*_connected.rb']
end