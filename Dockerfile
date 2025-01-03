FROM python:3.13.1-slim-bookworm

WORKDIR /usr/src/app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

EXPOSE 502

COPY emulator.py ./

CMD [ "/usr/local/bin/python", "./emulator.py" ]
