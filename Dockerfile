FROM python
COPY ustream/requirements.txt .
RUN pip install -r requirements.txt && rm requirements.txt
COPY ./* /ustream/
WORKDIR /ustream
ENV USTREAM_PUBLIC_URL "http://127.0.0.1:2138"
EXPOSE 2137
ENTRYPOINT ["python", "node.py"]
CMD ["/bin/bash"]
