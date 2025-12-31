# provider
provider "aws" {
  region = "eu-north-1"
  shared_credentials_files = ["./keys/terraform_demo_accessKeys"]
}

# S3 raw bucket
resource "aws_s3_bucket" "gh_raw" {
  bucket = "amzn-s3-gharchive-raw-rslob"
  force_destroy = true
}

resource "aws_s3_bucket" "gh_processed" {
  bucket = "amzn-s3-gharchive-processed-rslob"
  force_destroy = true
}