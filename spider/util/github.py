from github import Github
from spider.util.memcache import cached_property


class GitHubCli(object):
    """https://pygithub.readthedocs.io/en/latest/introduction.html"""

    def __init__(self, access_token, host_name):
        self.a_t = access_token
        self.host_name = host_name

    @cached_property
    def cli(self):
        if self.host_name:
            return Github(base_url=f"https://{self.host_name}/api/v3", login_or_token=self.a_t)
        else:
            return Github(login_or_token=self.a_t)

    def pull2path(self, repo, path):
        ...
