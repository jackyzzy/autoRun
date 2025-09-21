FROM python:3.12

WORKDIR /workspace/playbook
COPY . .

RUN pip install --no-cache-dir -r requirements.txt
RUN ssh-keygen -t rsa -b 4096 -f ~/.ssh/id_rsa -N ""

ENTRYPOINT ["python", "playbook.py"]