require 'mongo'
require 'log4r'

module Mongoriver; end

require 'mongoriver/version'
require 'mongoriver/log'
require 'mongoriver/assertions'

require 'mongoriver/tailer'
require 'mongoriver/tailer_stream_state'
require 'mongoriver/toku'
require 'mongoriver/abstract_persistent_tailer'
require 'mongoriver/persistent_tailer'
require 'mongoriver/abstract_outlet'
require 'mongoriver/stream'
