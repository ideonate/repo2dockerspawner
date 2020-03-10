"""
A Spawner for JupyterHub that runs each user's server in a separate docker container with repo2docker as an option
"""
from dockerspawner import DockerSpawner
from concurrent.futures import ThreadPoolExecutor
from async_generator import async_generator, yield_
from asyncio import sleep

import re
from collections import deque

import docker
from docker.errors import APIError
from tornado import gen

from traitlets import (
    Any,
    Bool,
    CaselessStrEnum,
    Dict,
    List,
    Int,
    Unicode,
    Union,
    default,
    observe,
    validate,
)

import jupyterhub

_jupyterhub_xy = "%i.%i" % (jupyterhub.version_info[:2])


class Repo2DockerSpawner(DockerSpawner):
    """A Spawner for JupyterHub that runs each user's server in a separate docker container
    with repo2docker as an option"""

    _build_executor = None

    @property
    def build_executor(self):
        """single global executor"""
        cls = self.__class__
        if cls._build_executor is None:
            cls._build_executor = ThreadPoolExecutor(5)
        return cls._build_executor

    log_generator = None

    @default('options_form')
    def _default_options_form(self):
        return """
        
        <input type="radio" id="use_r2d_yes" name="use_r2d" value="yes" checked>
        <label for="use_r2d_yes">Start server from a repository</label> <br />

        <label for="repourl">Repo URL:</label>
        <input type="text" name="repourl" value="" />
        
        </input>
        
        <br />
        
        <label for="reporef">Tag:</label>
        <input type="text" name="reporef" value="" />
        
        <br />
        <br />
        
        <input type="radio" id="use_r2d_no" name="use_r2d" value="no">
        <label for="use_r2d_no">Start an empty server</label> 
        </input>
        
        <br />
        <br />
        
        """

    def options_from_form(self, formdata):
        """Turn options formdata into user_options"""
        options = {}

        if 'repourl' in formdata:
            options['repourl'] = formdata['repourl'][0]

        if 'reporef' in formdata:
            options['reporef'] = formdata['reporef'][0]

        if 'use_r2d' in formdata:
            options['use_r2d'] = formdata['use_r2d'][0]

        return options

    def build_r2d(self, repourl, ref):

        class MyLogGen(object):
            # Really need to take care of buffer filling up

            def __init__(self):
                self.loglines = deque([])

            def __next__(self):
                try:
                    return self.loglines.popleft()
                except IndexError as e:
                    return

            def __iter__(self):
                return self

            def push(self, progress, logline):
                self.loglines.append({'progress': int(progress), 'message': logline})

        self.log.info("Make MyLogGen")

        self.log_generator = MyLogGen()

        r2d_image_name = "jupyter/repo2docker:0.10.0"

        self.log_generator.push(1, 'Creating repo2docker container {} for repo {} and ref {}'.format(r2d_image_name, repourl, ref))

        self.log.info("Make pull")

        self.pull_build_image(r2d_image_name)

        host_config = {
            'binds': {
                '/var/run/docker.sock': {
                    'bind': '/var/run/docker.sock',
                    'mode': 'rw',
                }
            },
            'auto_remove': False
        }

        self.log.info("Starting host with config: %s", host_config)

        host_config = self.client.create_host_config(**host_config)

        r2d_cmd = ['repo2docker', '--no-run']

        if ref:
            r2d_cmd.extend(['--ref', ref])

        r2d_cmd.extend(['--user-name=jovyan', '--user-id=1000', repourl])

        container = self._docker('create_container',
                                      image=r2d_image_name,
                                      host_config=host_config,
                                      command=r2d_cmd,
                                      environment={'PYTHONUNBUFFERED': '0'}) # Try to ensure all logs make it through

        container_id = container['Id']

        self.log_generator.push(1, 'Starting repo2docker container')

        self._docker('start', container_id)

        self.log.info(
            "r2d Container %s for %s",
            container_id[:12], self._log_name,
        )

        self.log_generator.push(1, 'Connecting to repo2docker container logs')

        # Track logs pretty much to the end, but the stream might break before truly finished
        docker_log_gen = self._docker('logs', container_id, stream=True, follow=True)

        self.log.info('Got docker log gen')

        image_name = self.follow_logs(docker_log_gen)

        self.log.info('Returned wrap_follow_logs')

        retval = self._docker('wait', container_id)

        statuscode = retval['StatusCode']

        if statuscode == 0 and image_name == '':
            self.log.info('TRYING TAIL LOGS to get image name')
            # We didn't pick up an image name, so try again with fixed logs at the end
            docker_log_gen = self._docker('logs', container_id, stream=False, follow=False, tail=5)
            docker_log_gen = docker_log_gen.decode('utf-8').split("\n")
            image_name = self.follow_logs(docker_log_gen, track_progress=False)

        self._docker('remove_container', container_id)

        self.log.info(
            "Awaited r2d Container %s for %s StatusCode %d",
            container_id[:12], self._log_name, statuscode
        )

        if statuscode == 0 and image_name == '':
            raise Exception('repo2docker did not provide a name for the image within its logs')

        return image_name

    def follow_logs(self, docker_log_gen, track_progress=True):

        step_regex = re.compile(r'^Step (\d+)/(\d+) : .*')
        tag_regex = re.compile(r'^Successfully tagged (([a-z0-9]+(?:[._-]{1,2}[a-z0-9]+)*)(:([a-z0-9]+(?:[._-]{1,2}[a-z0-9]+)*))?)\n?$')
        reuse_regex = re.compile(r'^Reusing existing image \((([a-z0-9]+(?:[._-]{1,2}[a-z0-9]+)*)(:([a-z0-9]+(?:[._-]{1,2}[a-z0-9]+)*))?)\), not building')

        image_name = ''

        # For progress, take 0-5% as the bit before we get any STEP message
        # 5-95 is spread between the STEPS (once we know how many steps)
        # within each block, each new logline pushes us asymptotically closer to the next bar
        step_0_pct = 5
        step_end_pct = 95

        (curstep, maxstep) = (0, 0)

        progress = 1
        next_progress_ceil = step_0_pct

        for logline in docker_log_gen:

            if track_progress:
               logline = logline.decode('utf-8')

            self.log.info(logline)

            if track_progress:
                # May get progress lines such as Step 3/10 : ....
                # or at end: Successfully tagged r2dhttps-3a-2f-2fgithub-2ecom-2fdanlester-2fr2d-2dskeleton:latest\n

                m = step_regex.match(logline)
                if m:
                    (curstep, maxstep) = m.groups()
                    self.log.info('NOW at {} of {}'.format(curstep, maxstep))

                    (curstep, maxstep) = (int(curstep), int(maxstep))

                    if maxstep == 0:
                        maxstep = 1

                    progress = (curstep - 1) * ( (step_end_pct - step_0_pct) / maxstep ) + step_0_pct

                    next_progress_ceil = curstep * ( (step_end_pct - step_0_pct) / maxstep ) + step_0_pct

                else:
                    # Since we don't know how many events we will get,
                    # asymptotically approach 90% completion with each event.
                    # each event gets 33% closer to 90%:
                    # 30 50 63 72 78 82 84 86 87 88 88 89
                    progress += (next_progress_ceil - progress) / 3

            m = tag_regex.match(logline)
            if not m:
                m = reuse_regex.match(logline)
            if m:
                image_name = m.group(1)
                self.log.info('FOUND IMAGE NAME: '+image_name)

            if track_progress:
                self.log_generator.push(progress, logline)

        return image_name

    @async_generator
    async def progress(self):
        """
        This function is reporting back the progress of spawning a pod until
        self._start_future has fired.
        This is working with events parsed by the python kubernetes client,
        and here is the specification of events that is relevant to understand:
        ref: https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.16/#event-v1-core
        """

        spawn_future = self._spawn_future

        break_while_loop = False
        progress = 0
        while True:
            self.log.debug('progress generator')

            if spawn_future.done():
                self.log.debug('progress generator BREAK spawn_future.done')
                break_while_loop = True

            if self.log_generator:

                i = 0

                self.log.debug('START LOOP progress generator')

                for logdict in self.log_generator:
                    if logdict is None:
                        await sleep(1)
                    else:
                        #self.log.debug(str(logdict))

                        await yield_(logdict)

                        i = i + 1

                self.log.debug('END LOOP progress generator')

            if break_while_loop:
                self.log.debug('BREAK LOOP progress generator')
                break

            await sleep(1)
            self.log.debug('AWAIT SLEEP progress generator')

    def pull_build_image(self, image):
        """Pull the image, ifnotpresent
        Pull on this thread
        """
        # docker wants to split repo:tag
        # the part split("/")[-1] allows having an image from a custom repo
        # with port but without tag. For example: my.docker.repo:51150/foo would not
        # pass this test, resulting in image=my.docker.repo:51150/foo and tag=latest
        if ':' in image.split("/")[-1]:
            # rsplit splits from right to left, allowing to have a custom image repo with port
            repo, tag = image.rsplit(':', 1)
        else:
            repo = image
            tag = 'latest'

        try:
            # check if the image is present
            self._docker('inspect_image', image)
        except docker.errors.NotFound:
            # not present, pull it for the first time
            self.log.info("pulling image %s", image)
            self._docker('pull', repo, tag)

    @gen.coroutine
    def start(self, image=None, extra_create_kwargs=None, extra_host_config=None):
        """Start the single-user server in a docker container.

        Additional arguments to create/host config/etc. can be specified
        via .extra_create_kwargs and .extra_host_config attributes.

        If the container exists and `c.DockerSpawner.remove` is true, then
        the container is removed first. Otherwise, the existing containers
        will be restarted.
        """

        if image:
            self.log.warning("Specifying image via .start args is deprecated")
            self.image = image
        if extra_create_kwargs:
            self.log.warning(
                "Specifying extra_create_kwargs via .start args is deprecated"
            )
            self.extra_create_kwargs.update(extra_create_kwargs)
        if extra_host_config:
            self.log.warning(
                "Specifying extra_host_config via .start args is deprecated"
            )
            self.extra_host_config.update(extra_host_config)

        # image priority:
        # 1. user options (from spawn options form)
        # 2. self.image from config
        image_option = self.user_options.get('image')
        if image_option:
            # save choice in self.image
            self.image = yield self.check_image_whitelist(image_option)

        use_r2d = self.user_options.get('use_r2d')

        if use_r2d == 'yes':

            repourl = self.user_options.get('repourl')
            ref = self.user_options.get('reporef')

            self.cmd = 'jupyterhub-singleuser'

            #self.image = yield self.build_r2d(repourl, ref)

            self.log.info('about to build future')

            build_future = yield self.build_executor.submit(self.build_r2d, repourl, ref)
            self.log.info(build_future)

            #wait([build_future])
            image_name = build_future #.result()
            self.log.info(image_name)
            self.image = image_name

        else:

            yield self.pull_image(self.image)

        # Should be able to call super class here?

        obj = yield self.get_object()
        if obj and self.remove:
            self.log.warning(
                "Removing %s that should have been cleaned up: %s (id: %s)",
                self.object_type,
                self.object_name,
                self.object_id[:7],
            )
            yield self.remove_object()

            obj = None

        if obj is None:
            obj = yield self.create_object()
            self.object_id = obj[self.object_id_key]
            self.log.info(
                "Created %s %s (id: %s) from image %s",
                self.object_type,
                self.object_name,
                self.object_id[:7],
                self.image,
            )

        else:

            self.log.info(
                "Found existing %s %s (id: %s)",
                self.object_type,
                self.object_name,
                self.object_id[:7],
            )
            # Handle re-using API token.
            # Get the API token from the environment variables
            # of the running container:
            for line in obj["Config"]["Env"]:
                if line.startswith(("JPY_API_TOKEN=", "JUPYTERHUB_API_TOKEN=")):
                    self.api_token = line.split("=", 1)[1]
                    break

        # TODO: handle unpause
        self.log.info(
            "Starting %s %s (id: %s)",
            self.object_type,
            self.object_name,
            self.container_id[:7],
        )

        # start the container
        yield self.start_object()

        if hasattr(self, 'post_start_cmd') and self.post_start_cmd:
            yield self.post_start_exec()

        ip, port = yield self.get_ip_and_port()
        if jupyterhub.version_info < (0, 7):
            # store on user for pre-jupyterhub-0.7:
            self.user.server.ip = ip
            self.user.server.port = port
        # jupyterhub 0.7 prefers returning ip, port:
        return (ip, port)
