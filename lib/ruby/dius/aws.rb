require 'rubygems'
require 'aws'

module Dius
  module Aws

    AWS_ACCESS_KEY = ENV['AWS_ACCESS_KEY']
    AWS_SECRET_KEY = ENV['AWS_SECRET_KEY']
    AWS_REGION = 'us-east-1'

    usage = """
      Please set environment variables AWS_ACCESS_KEY, AWS_SECRET_KEY and AWS_REGION.

      For example:
      export AWS_ACCESS_KEY=AHUYGGYUDKGYUFKHU
      export AWS_SECRET_KEY=892360fyh&(NT69N7y80r238
      export AWS_REGION=us-east-1
    """

    unless AWS_ACCESS_KEY && AWS_SECRET_KEY && AWS_REGION
      puts usage
      exit 1
    end

    AWS.config(:access_key_id => AWS_ACCESS_KEY, :secret_access_key => AWS_SECRET_KEY)

    def s3
      s3 = AWS::S3.new
    end

    def emr
      AWS::EMR.new(:region => AWS_REGION)
    end

    def s3_put(file, s3_path)
      if s3_path.start_with?('s3://')
        s3_path = s3_path[5..-1]
      end

      bucket = s3_path.slice(0, s3_path.index("/"))
      path = s3_path.slice(s3_path.index("/") + 1, s3_path.length)

      object = s3.buckets[bucket].objects[path + "/" + File.basename(file)]
      object.write(Pathname.new(file))
    end


  end
end
