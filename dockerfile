FROM python:3.7.4
COPY src/ /app
WORKDIR /app
RUN pip install -r requirements.txt
RUN ln -snf /usr/share/zoneinfo/America/Mexico_City /etc/localtime && echo America/Mexico_City > /etc/timezone
ENTRYPOINT ["python"]
CMD ["main.py"]