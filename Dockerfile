FROM python:3.13.1-slim-bookworm

WORKDIR /usr/src/app

# expose external port
EXPOSE 502

# install requirements with temporary requirements.txt
RUN --mount=type=bind,source=requirements.txt,target=/tmp/requirements.txt \
    pip install --requirement /tmp/requirements.txt

# set up non-root user
RUN groupadd -r vcontrold && useradd --no-log-init -r -g vcontrold vcontrold
USER vcontrold

# copy code
COPY emulator.py ./

# start emulator
CMD [ "/usr/local/bin/python", "./emulator.py" ]
