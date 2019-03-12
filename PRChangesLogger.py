"""
    @f1r3f0x - 08/03/2019
    License: MIT

    Azure DevOps PRChangesLogger:
    Creates a file with all the changes in pull request for specific branches.
"""

from azure.devops.connection import Connection
from azure.devops.released import git
from azure.devops import exceptions as AZExceptions
from msrest.authentication import BasicAuthentication
from msrest import exceptions as MSExceptions
from azure.devops.v5_0.git.models import GitPullRequestSearchCriteria
import openpyxl as xl
from tqdm import tqdm

from pprint import pprint
from datetime import datetime
from concurrent.futures.thread import ThreadPoolExecutor
from concurrent import futures

MAX_WORKERS = 16  # This will determine of pull request being processed at a time


# Fill in with your personal access token and org URL
access_token = 'yourtoken12345' # Be careful with this
organization_url = 'https://yourorgurl.com'

repository_name = 'hello_world'

source_branch_name = 'dev'
target_branch_name = 'master'

pull_quantity = 99999


# Worker function
def process_pull_requests(git_client, repo_id, pull):
    processed_changes = {}
    commits = git_client.get_pull_request_commits(repo_id, pull.pull_request_id)
    
    for commit in commits:
        changes = git_client.get_changes(commit.commit_id, repo_id).changes
        for change in changes:
            #print('\t ', change['item']['path'])
            file_name = change['item']['path']

            if '.' in file_name:
                counter = processed_changes.get(file_name)
                if not counter:
                    counter = 0
                counter +=1
                processed_changes[file_name] = counter
    return processed_changes


def get_changes(access_token, organization_url, target_repo, source_branch_name, target_branch_name, pull_quantity):
    print('\nConnecting to API\n')
    try:
        # Create a connection to the org
        credentials = BasicAuthentication('', access_token)
        connection = Connection(base_url=organization_url, creds=credentials)

        # Get git Client
        # See azure.devops.v5_0.models for models
        #     azure.devops.git.git_client_base for git_client methods
        git_client = connection.clients.get_git_client()

        # Get the repo
        repositories = git_client.get_repositories()

    except MSExceptions.ClientRequestError as err:
        print('Client Request Error:', str(err))
        return None
    except MSExceptions.AuthenticationError as err:
        print('Authentication Error: ', str(err))

    repo_travesia = None
    for repo in repositories:
        #pprint.pprint(repo.__dict__)
        if repo.name == target_repo:
            repo_travesia = repo
    
    if not repo_travesia:
        print(f'Repository {repository_name} not found.')
        return None

    # Find commits for the specific branch combination
    search_criteria = GitPullRequestSearchCriteria (
        source_ref_name = f'refs/heads/{source_branch_name}',
        target_ref_name = f'refs/heads/{target_branch_name}',
        status = 'Completed'
    )

    pull_requests = git_client.get_pull_requests(repo_travesia.id, search_criteria, top=9999)

    all_changes = {}

    print("Getting Changes ..")
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_prs = { executor.submit(process_pull_requests, git_client, repo_travesia.id, pull): pull for pull in pull_requests}
        for future in tqdm(futures.as_completed(future_prs), unit=' Changes'):
            data = future.result()
            for change in data.keys():
                if all_changes.get(change):
                    all_changes[change] = all_changes[change] + data[change]
                else:
                    all_changes[change] = data[change]
    print()

    #print(all_changes)


    """
    print('Getting changes')
    for pull in tqdm(pull_requests):
        #pprint(pull.pull_request_id)
        #pprint(pull.title)
        #print(pull.creation_date)
        #pprint(pull.url)
        commits = git_client.get_pull_request_commits(repo_travesia.id, pull.pull_request_id)
        
        for commit in commits:
            changes = git_client.get_changes(commit.commit_id, repo_travesia.id).changes
            for change in changes:
                #print('\t ', change['item']['path'])
                file_name = change['item']['path']

                if '.' in file_name:
                    counter = all_changes.get(file_name)
                    if not counter:
                        counter = 0
                    counter +=1
                    all_changes[file_name] = counter
    """

    #pprint(all_changes)
    return all_changes


def create_workbook(source_branch_name, target_branch_name, changes, workbook_title='output.xlsx'): 
    print('Creating Excel file...')
    
    wb = xl.load_workbook(filename='format.xlsx', data_only=True)
    sheet = wb.active

    sheet['A1'] = f'{source_branch_name}   ->   {target_branch_name}'
    sheet['D1'] = 'Generated by PRChangesLogger.py by @f1r3f0x'

    for i, file_name in enumerate(tqdm(sorted(changes.keys()), unit='rows')):
        row_index = i + 3
        sheet.cell(row=row_index, column=1, value=file_name)
        sheet.cell(row=row_index, column=2, value='-')

    file_name = f'{source_branch_name}_to_{target_branch_name}-{datetime.now().timestamp()}.xlsx'

    print(f'Workbook saved as {file_name}\n')
    wb.save(file_name)

    return wb


if __name__ == "__main__":
    import json

    print('\nF1r3f0x\'s Azure DevOps PR changes logger\n')

    valid_file = True
    try:
        config = json.load(open('config.json'))
        access_token = config['access_token']
        organization_url = config['organization_url']
        repository_name = config['repository_name']
        pull_quantity = int(config['pull_quantity'])
    except FileNotFoundError as err:
        print('Config file not found')
        if input('Do you want to create a new one? (Y/N) ').strip().lower() == 'y':
            access_token = input('Access Token: ')
            organization_url = input('Organization URL: ')
            repository_name = input('Repository Name: ')
            json.dump({
                'access_token': access_token,
                'organization_url': organization_url,
                'repository_name': repository_name,
                'pull_quantity': pull_quantity
            }, open('config.json', 'w'))
            print('done!')
        else:
            valid_file = False
    except json.JSONDecodeError as err:
        print('JSON Decoding Error -', str(err))
        valid_file = False
    except KeyError as err:
        print('Error in JSON Keys:', str(err))
        valid_file = False
    except ValueError as err:
        print('Pull quantity must be an Int')
        valid_file = False

    if valid_file:
        source_branch_name = input('Source Branch name: ')
        target_branch_name = input('Target Branch name: ')

        changes = get_changes(access_token, organization_url, repository_name,
            source_branch_name, target_branch_name, pull_quantity)

        if changes:
            workbook = create_workbook(source_branch_name, target_branch_name, changes)
        else:
            print('No changes found')

    print('by @f1r3f0x - https://github.com/F1r3f0x\n')
    input('press any key to close ...\n\n')