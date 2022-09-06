# You can find the new timestamped tags here: https://hub.docker.com/r/gitpod/workspace-full/tags
FROM gitpod/workspace-full:2022-07-12-11-05-29

# Install custom tools, runtime, etc.
RUN sudo apt install -y  libsasl2-dev kafkacat