FROM python:3.9.1

ADD . /python-flask
WORKDIR /python-flask

RUN pip3 install flask
RUN pip3 install faker
RUN pip3 install geopy
RUN pip3 install pandas
RUN pip3 install tornado
RUN pip3 install bokeh
RUN pip3 install numpy
RUN pip3 install requests

COPY . .
ENTRYPOINT [ "python" ]
CMD [ "main.py" ]