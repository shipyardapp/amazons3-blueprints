import os
import boto3
import botocore
from botocore.client import Config
import re
import argparse
import glob
from ast import literal_eval
import sys


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
        '--destination-folder-name',
        dest='destination_folder_name',
        default='',
        required=False)
    parser.add_argument(
        '--destination-file-name',
        dest='destination_file_name',
        default=None,
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



def connect_to_s3(access_key_id, secret_access_key, default_region=None):
    """
    Create a connection to the S3 service using credentials provided as environment variables.
    """
    session = boto3.Session(
        aws_access_key_id=access_key_id,
        aws_secret_access_key=secret_access_key,
        region_name=default_region
    )

    s3_connection = session.resource('s3')
    return s3_connection


def move_s3_file(
        s3_connection,
        source_bucket_name,
        destination_bucket_name,
        source_full_path,
        destination_full_path,
        ):
    """
    Moves an AWS S3 file from one bucket to another.

    The specific way it does this is by first copying the file from one bucket to another
    then deleting the file in the source_bucket.
    """
    #create a source dictionary that specifies bucket name and key name of the object to be copied
    copy_source = {
        'Bucket': source_bucket_name,
        'Key': source_full_path
    }

    bucket = s3.Bucket(destination_bucket_name)
    bucket.copy(copy_source, destination_full_path)

    s3.Object(source_bucket_name, source_full_path).delete()

    print(f'{source_full_path} successfully uploaded to {bucket_name}/{destination_full_path}')


def main():
    args = get_args()
    set_environment_variables(args)
    bucket_name = args.bucket_name
    source_file_name = args.source_file_name
    source_folder_name = args.source_folder_name
    source_full_path = combine_folder_and_file_name(
                            source_folder_name,
                            source_file_name)
    destination_folder_name = clean_folder_name(args.destination_folder_name)
    source_file_name_match_type = args.source_file_name_match_type
    s3_config = args.s3_config

    s3_connection = connect_to_s3(s3_config)

    if source_file_name_match_type == 'regex_match':
        file_names = find_all_local_file_names(source_folder_name)
        matching_file_names = find_all_file_matches(
            file_names, re.compile(source_file_name))
        num_matches = len(matching_file_names)

        if num_matches == 0:
            print(f'No matches found for regex {source_file_name}')
            sys.exit(1)
        else:
            print(f'{num_matches} files found. Preparing to upload...')

        for index, key_name in enumerate(matching_file_names):
            destination_full_path = determine_destination_full_path(
                destination_folder_name=destination_folder_name,
                destination_file_name=args.destination_file_name,
                source_full_path=key_name,
                file_number=None if num_matches == 1 else index + 1)
            print(f'Uploading file {index+1} of {len(matching_file_names)}')
            upload_s3_file(
                source_full_path=key_name,
                destination_full_path=destination_full_path,
                bucket_name=bucket_name,
                extra_args=extra_args,
                s3_connection=s3_connection)

    else:
        destination_full_path = determine_destination_full_path(
            destination_folder_name=destination_folder_name,
            destination_file_name=args.destination_file_name,
            source_full_path=source_full_path)
        upload_s3_file(
            source_full_path=source_full_path,
            destination_full_path=destination_full_path,
            bucket_name=bucket_name,
            extra_args=extra_args,
            s3_connection=s3_connection)


if __name__ == '__main__':
    main()
