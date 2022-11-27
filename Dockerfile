FROM python
COPY ustream/requirements.txt .
RUN pip install -r requirements.txt && rm requirements.txt
COPY ./* /ustream/
WORKDIR /ustream
EXPOSE 2137
ENTRYPOINT ["python", "server.py"]
CMD ["/bin/bash"]
