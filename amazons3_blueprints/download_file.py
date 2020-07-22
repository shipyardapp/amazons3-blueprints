import os
import boto3
import botocore
from botocore.client import Config
import re
import argparse
import code


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--bucket-name', dest='bucket_name', required=True)
    parser.add_argument(
        '--source-file-name-match-type',
        dest='source_file_name_match_type',
        choices={
            'exact_match',
            'regex_match'},
        required=True)
    parser.add_argument(
        '--source-folder-name',
        dest='source_folder_name',
        default='',
        required=False)
    parser.add_argument(
        '--source-file-name',
        dest='source_file_name',
        required=True)
    parser.add_argument(
        '--destination-file-name',
        dest='destination_file_name',
        default=None,
        required=False)
    parser.add_argument(
        '--destination-folder-name',
        dest='destination_folder_name',
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


def extract_file_name_from_source_full_path(source_full_path):
    """
    Use the file name provided in the source_file_name variable. Should be run only
    if a destination_file_name is not provided.
    """
    destination_file_name = os.path.basename(source_full_path)
    return destination_file_name


def enumerate_destination_file_name(destination_file_name, file_number=1):
    """
    Append a number to the end of the provided destination file name.
    Only used when multiple files are matched to, preventing the destination file from being continuously overwritten.
    """
    if re.search(r'\.', destination_file_name):
        destination_file_name = re.sub(
            r'\.', f'_{file_number}.', destination_file_name, 1)
    else:
        destination_file_name = f'{destination_file_name}_{file_number}'
    return destination_file_name


def determine_destination_file_name(
    *,
    source_full_path,
    destination_file_name,
        file_number=None):
    """
    Determine if the destination_file_name was provided, or should be extracted from the source_file_name,
    or should be enumerated for multiple file downloads.
    """
    if destination_file_name:
        if file_number:
            destination_file_name = enumerate_destination_file_name(
                destination_file_name, file_number)
        else:
            destination_file_name = destination_file_name
    else:
        destination_file_name = extract_file_name_from_source_full_path(
            source_full_path)

    return destination_file_name


def clean_folder_name(folder_name):
    """
    Cleans folders name by removing duplicate '/' as well as leading and trailing '/' characters.
    """
    folder_name = folder_name.strip('/')
    if folder_name != '':
        folder_name = os.path.normpath(folder_name)
    return folder_name


def combine_folder_and_file_name(folder_name, file_name):
    """
    Combine together the provided folder_name and file_name into one path variable.
    """
    combined_name = os.path.normpath(
        f'{folder_name}{"/" if folder_name else ""}{file_name}')
    combined_name = os.path.normpath(combined_name)

    return combined_name


def determine_destination_name(
        destination_folder_name,
        destination_file_name,
        source_full_path,
        file_number=None):
    """
    Determine the final destination name of the file being downloaded.
    """
    destination_file_name = determine_destination_file_name(
        destination_file_name=destination_file_name,
        source_full_path=source_full_path,
        file_number=file_number)
    destination_name = combine_folder_and_file_name(
        destination_folder_name, destination_file_name)
    return destination_name


def list_s3_objects(
        s3_connection,
        bucket_name,
        prefix='',
        continuation_token=None):
    """
    List 1000 objects at a time, filtering by the prefix and continuing if more than 1000
    objects were found on the previous run.
    """
    kwargs = {'Bucket': bucket_name, 'Prefix': prefix}
    if continuation_token:
        kwargs['ContinuationToken'] = continuation_token

    response = s3_connection.list_objects_v2(**kwargs)
    return response


def find_s3_file_names(response):
    """
    Return all the objects found on S3 as a list.
    """
    file_names = []
    objects = response['Contents']
    for obj in objects:
        object = obj['Key']
        file_names.append(object)

    return file_names


def find_all_s3_file_names(s3_connection, bucket_name, source_folder_name=''):
    """
    Run the find_s3_file_names() in a loop until no more continuation tokens are found.
    Return a list of all source_full_paths.
    """
    response = list_s3_objects(
        s3_connection=s3_connection,
        bucket_name=bucket_name,
        prefix=source_folder_name)
    file_names = find_s3_file_names(response)
    continuation_token = response.get('NextContinuationToken')

    while continuation_token:
        response = list_s3_objects(
            s3_connection=s3_connection,
            bucket_name=bucket_name,
            prefix=source_folder_name,
            continuation_token=continuation_token)
        file_names = file_names.append(find_s3_file_names(response))
        continuation_token = response.get('NextContinuationToken')
    return file_names


def find_all_file_matches(file_names, file_name_re):
    """
    Return a list of all file_names that matched the regular expression.
    """
    matching_file_names = []
    for file in file_names:
        if re.search(file_name_re, file):
            matching_file_names.append(file)

    return matching_file_names


def download_s3_file(
        s3_connection,
        bucket_name,
        source_full_path,
        destination_file_name=None):
    """
    Download a selected file from S3 to local storage in the current working directory.
    """
    local_path = os.path.normpath(f'{os.getcwd()}/{destination_file_name}')

    s3_connection.download_file(bucket_name, source_full_path, local_path)

    print(f'{bucket_name}/{source_full_path} successfully downloaded to {local_path}')

    return


def main():
    args = get_args()
    set_environment_variables(args)
    bucket_name = args.bucket_name
    source_file_name = args.source_file_name
    source_folder_name = clean_folder_name(args.source_folder_name)
    source_full_path = combine_folder_and_file_name(
        folder_name=source_folder_name, file_name=source_file_name)
    source_file_name_match_type = args.source_file_name_match_type
    s3_config = args.s3_config
    destination_folder_name = clean_folder_name(args.destination_folder_name)

    if not os.path.exists(destination_folder_name) and (
            destination_folder_name != ''):
        os.makedirs(destination_folder_name)

    s3_connection = connect_to_s3(s3_config)

    if source_file_name_match_type == 'regex_match':
        file_names = find_all_s3_file_names(
            s3_connection=s3_connection,
            bucket_name=bucket_name,
            source_folder_name=source_folder_name)
        matching_file_names = find_all_file_matches(
            file_names, re.compile(source_file_name))
        print(f'{len(matching_file_names)} files found. Preparing to download...')

        for index, key_name in enumerate(matching_file_names):
            destination_name = determine_destination_name(
                destination_folder_name=destination_folder_name,
                destination_file_name=args.destination_file_name,
                source_full_path=key_name,
                file_number=index + 1)
            print(f'Downloading file {index+1} of {len(matching_file_names)}')
            download_s3_file(
                bucket_name=bucket_name,
                source_full_path=key_name,
                destination_file_name=destination_name,
                s3_connection=s3_connection)
    else:
        destination_name = determine_destination_name(
            destination_folder_name=destination_folder_name,
            destination_file_name=args.destination_file_name,
            source_full_path=source_full_path)
        download_s3_file(
            bucket_name=bucket_name,
            source_full_path=source_full_path,
            destination_file_name=destination_name,
            s3_connection=s3_connection)


if __name__ == '__main__':
    main()
