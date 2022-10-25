import os
import boto3
import botocore
from botocore.client import Config
import re
import argparse
import glob
from ast import literal_eval
import sys
import shipyard_utils as shipyard
try:
    import exit_codes as ec
except BaseException:
    from . import exit_codes as ec


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--bucket-name', dest='bucket_name', required=True)
    parser.add_argument(
        '--source-file-name-match-type',
        dest='source_file_name_match_type',
        default='exact_match',
        choices={
            'exact_match',
            'regex_match'},
        required=False)
    parser.add_argument(
        '--source-file-name',
        dest='source_file_name',
        required=True)
    parser.add_argument(
        '--source-folder-name',
        dest='source_folder_name',
        default='',
        required=False)
    parser.add_argument(
        '--s3-config',
        dest='s3_config',
        default=None,
        required=False)
    parser.add_argument(
        '--aws-access-key-id',
        dest='aws_access_key_id',
        required=False)
    parser.add_argument(
        '--aws-secret-access-key',
        dest='aws_secret_access_key',
        required=False)
    parser.add_argument(
        '--aws-default-region',
        dest='aws_default_region',
        required=False)
    return parser.parse_args()


def set_environment_variables(args):
    """
    Set AWS credentials as environment variables if they're provided via keyword arguments
    rather than seeded as environment variables. This will override system defaults.
    """

    if args.aws_access_key_id:
        os.environ['AWS_ACCESS_KEY_ID'] = args.aws_access_key_id
    if args.aws_secret_access_key:
        os.environ['AWS_SECRET_ACCESS_KEY'] = args.aws_secret_access_key
    if args.aws_default_region:
        os.environ['AWS_DEFAULT_REGION'] = args.aws_default_region
    return


def connect_to_s3(s3_config=None):
    """
    Create a connection to the S3 service using credentials provided as environment variables.
    """
    s3_connection = boto3.client(
        's3',
        config=Config(s3_config)
    )
    return s3_connection


def s3_list_files(
        s3_connection,
        bucket_name,
        source_folder,
        ):
    """List files in s3"""
    s3_response = s3_connection.list_objects_v2(Bucket=bucket_name, Prefix=source_folder)
    files_list =  [
        _file['Key'] for _file in s3_response['Contents']
    ]
    return files_list


def remove_s3_file(
        s3_connection,
        bucket_name,
        source_full_path,
        ):
    """
    Uploads a single file to S3. Uses the s3.transfer method to ensure that files larger than 5GB are split up during the upload process.

    Extra Args can be found at https://boto3.amazonaws.com/v1/documentation/api/latest/guide/s3-uploading-files.html#the-extraargs-parameter
    and are commonly used for custom file encryption or permissions.
    """
    try:
        s3_response = s3_connection.delete_object(
            Bucket=bucket_name,
            Key=source_full_path
        )

        print(f'{source_full_path} delete function successful')
    except Exception as e:
        print(f"Error: {source_full_path} not found in bucket {bucket_name}.")
        sys.exit(ec.EXIT_CODE_FILE_NOT_FOUND)


def main():
    args = get_args()
    set_environment_variables(args)
    bucket_name = args.bucket_name
    source_file_name = args.source_file_name
    source_folder_name = shipyard.files.clean_folder_name(args.source_folder_name)
    source_full_path = shipyard.files.combine_folder_and_file_name(
        source_folder_name, source_file_name
    )
    source_file_name_match_type = args.source_file_name_match_type
    s3_config = args.s3_config

    s3_connection = connect_to_s3(s3_config)
    if source_file_name_match_type == 'regex_match':
        file_names = s3_list_files(
            s3_connection, bucket_name, source_folder_name)
        ## exit if there is a regex error
        try:
            matching_file_names = shipyard.files.find_all_file_matches(
                file_names, source_file_name)
            num_matches = len(matching_file_names)
        except Exception as e:
            print(f"Error in finding regex matches. Please make sure a valid regex is entered")
            sys.exit(ec.EXIT_CODE_INVALID_REGEX)

        if num_matches == 0:
            print(f'No matches found for regex {source_file_name}')
            sys.exit(1)
        else:
            print(f'{num_matches} files found. Preparing to remove...')

        for index, key_name in enumerate(matching_file_names,1):
            remove_s3_file(
                source_full_path=key_name,
                bucket_name=bucket_name,
                s3_connection=s3_connection
            )
            print(f'Removing file {index} of {len(matching_file_names)}')

    else:
        remove_s3_file(
            source_full_path=source_full_path,
            bucket_name=bucket_name,
            s3_connection=s3_connection
        )


if __name__ == '__main__':
    main()
