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
        '--source-bucket-name',
        dest='source_bucket_name',
        default='',
        required=True)
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
        '--destination-bucket-name',
        dest='destination_bucket_name',
        default='',
        required=True)
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


def s3_list_files(
        s3_connection,
        bucket_name,
        source_folder,
        ):
    """List files in s3"""
    try:
        # s3_response = s3_connection.list_objects_v2(Bucket=bucket_name, Prefix=source_folder)
        # files_list =  [
        #     _file['Key'] for _file in s3_response['Contents']
        # ]
        bucket = s3_connection.Bucket(bucket_name)

        files_list = [obj.key for obj in bucket.objects.filter(Prefix = source_folder)]
        return files_list
    except: 
        print(f"There was an error locating the files. Either the bucket does not exist or the folder does not exist. Please ensure that both are correct.")
        sys.exit(ec.EXIT_CODE_FILE_NOT_FOUND)


def connect_to_s3(access_key_id, secret_access_key, default_region=None):
    """
    Create a connection to the S3 service using credentials provided as environment variables.
    """
    try:
        session = boto3.Session(
            aws_access_key_id=access_key_id,
            aws_secret_access_key=secret_access_key,
            region_name=default_region
        )

        s3_connection = session.resource('s3')
        return s3_connection
    except Exception as e:
        print("Error: Could not connect to S3. Ensure that the provided access key, secret key, and region are correct")
        print(e)
        sys.exit(ec.EXIT_CODE_INVALID_CREDENTIALS)


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

    bucket = s3_connection.Bucket(destination_bucket_name)
    try: 
        bucket.copy(copy_source, destination_full_path)

        s3_connection.Object(source_bucket_name, source_full_path).delete()

        print(f'{source_full_path} successfully moved to {destination_bucket_name}/{destination_full_path}')
    except Exception as e:
        print(f"An error occured {e}.") 
        print(f"The file {source_bucket_name}/{source_full_path} could not be found")
        sys.exit(ec.EXIT_CODE_FILE_NOT_FOUND)

def main():
    args = get_args()
    set_environment_variables(args)
    source_file_name = args.source_file_name
    source_folder_name = args.source_folder_name
    source_full_path = shipyard.files.combine_folder_and_file_name(
                            source_folder_name,
                            source_file_name)
    destination_folder_name = shipyard.files.clean_folder_name(args.destination_folder_name)
    source_file_name_match_type = args.source_file_name_match_type
    # get arguments from OS environment
    aws_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
    aws_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']
    aws_default_region = os.environ['AWS_DEFAULT_REGION']
    source_bucket_name = args.source_bucket_name
    destination_bucket_name = args.destination_bucket_name

    s3_connection = connect_to_s3(
        aws_access_key_id, 
        aws_secret_access_key, 
        aws_default_region
        )

    if source_file_name_match_type == 'regex_match':
        file_names = s3_list_files(
            s3_connection, source_bucket_name, source_folder_name)
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
            print(f'{num_matches} files found. Preparing to upload...')

        try:
            for index, key_name in enumerate(matching_file_names,1):
                dest_file_name = shipyard.files.determine_destination_file_name(source_full_path = key_name,destination_file_name = None)
                destination_full_path = shipyard.files.determine_destination_full_path(
                    destination_folder_name = destination_folder_name,
                    destination_file_name = dest_file_name,
                    source_full_path = key_name
                )
                # destination_full_path = shipyard.files.combine_folder_and_file_name(
                #     destination_folder_name, key_name
                # )
                print(f'Moving file {index} of {len(matching_file_names)}')
                move_s3_file(
                        s3_connection,
                        source_bucket_name,
                        destination_bucket_name,
                        key_name,
                        destination_full_path
                )
        except Exception as e:
            print("Something went wrong moving the files")
            print(e)
            sys.exit(1)

    else:
        destination_file_name = args.destination_file_name
        destination_full_path = shipyard.files.determine_destination_full_path(
            destination_folder_name = destination_folder_name,
            destination_file_name = destination_file_name,
            source_full_path = source_full_path
        )
        # destination_full_path = shipyard.files.combine_folder_and_file_name(
        #     destination_folder_name, destination_file_name
        # )

        move_s3_file(
            s3_connection,
            source_bucket_name,
            destination_bucket_name,
            source_full_path,
            destination_full_path
        )


if __name__ == '__main__':
    main()
