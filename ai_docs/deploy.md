# Описание
Данный файл содержит требования по настройке доставки приложения на исполняемую среду. 

# Требования
- Сервис должен быть подготовлен для доставки в Docker-контейнер и для управления через GitLab action. 
-- Сервис должен поддерживать обновление секретов через GitLab Action с последующей перезагрузкой сервиса. 
-- Сервис должен содержать .dockerignore воизбежание упаковки секретов и лишних данных
- Сервис должен содержать инструкции по запуску на локальной машине. 

# Примеры рабочего проекта прежде
## deploy.yaml
``` yaml 
name: Deploy to VPS

on:
  push:
    branches:
      - master

jobs:
  deploy:
    runs-on: ubuntu-latest

    steps:
      - name: Checkout code
        uses: actions/checkout@v4

      - name: Set up SSH
        uses: webfactory/ssh-agent@v0.9.0
        with:
          ssh-private-key: ${{ secrets.SSH_PRIVATE_KEY }}

      - name: Deploy to server
        run: |
          # Устанавливаем переменные окружения
          export AMPLITUDE_CLIENT_ID="${{ secrets.AMPLITUDE_CLIENT_ID }}"
          export AMPLITUDE_SECRET_ID="${{ secrets.AMPLITUDE_SECRET_ID }}"
          export AWS_ACCESS_KEY_ID="${{ secrets.AWS_ACCESS_KEY_ID }}"
          export AWS_SECRET_ACCESS_KEY="${{ secrets.AWS_SECRET_ACCESS_KEY }}"
          export AWS_DEFAULT_REGION="${{ secrets.AWS_DEFAULT_REGION }}"
          export S3_ENDPOINT_URL="${{ secrets.S3_ENDPOINT_URL }}"
          export S3_BUCKET_NAME="${{ secrets.S3_BUCKET_NAME }}"
          export DEBUG="${{ secrets.DEBUG }}"

          # Подключаемся к серверу
          ssh -o StrictHostKeyChecking=no ${{ secrets.SSH_USER }}@${{ secrets.SSH_HOST }} << EOF
            set -e

            # Создаём директорию проекта, если её нет
            mkdir -p "${{ secrets.APP_PATH }}"
            cd "${{ secrets.APP_PATH }}"

            # Проверяем наличие репозитория и обновляем его
            if [ ! -d ".git" ]; then
              git clone git@github.com:alevch10/amplitude_downloader.git .
            else
              git fetch origin
              git reset --hard HEAD
              git clean -xdf
              git checkout master
              git pull origin master
            fi

            # Установливаем Poetry, если оно отсутствует
            if ! command -v /root/.local/bin/poetry &>/dev/null; then
              curl -sSL https://install.python-poetry.org | python3 -
            fi

            # Создаём виртуальное окружение внутри проекта
            /root/.local/bin/poetry config virtualenvs.in-project true
            /root/.local/bin/poetry install --only main
            /root/.local/bin/poetry lock

            # Создаём .env с секретами
            printf "AMPLITUDE_CLIENT_ID=%s\nAMPLITUDE_SECRET_ID=%s\nAWS_ACCESS_KEY_ID=%s\nAWS_SECRET_ACCESS_KEY=%s\nAWS_DEFAULT_REGION=%s\nS3_ENDPOINT_URL=%s\nS3_BUCKET_NAME=%s\nDEBUG=%s\n" \
              "$AMPLITUDE_CLIENT_ID" \
              "$AMPLITUDE_SECRET_ID" \
              "$AWS_ACCESS_KEY_ID" \
              "$AWS_SECRET_ACCESS_KEY" \
              "$AWS_DEFAULT_REGION" \
              "$S3_ENDPOINT_URL" \
              "$S3_BUCKET_NAME" \
              "$DEBUG" > .env

            # Перезапускаем приложение через systemd
            sudo systemctl daemon-reload || true
            sudo systemctl restart fastapi-app || true
          EOF
```

## Readme.md
``` md
В открытый файл вставить приватный ключ

Задать использование ключа для хоста:

ssh-keyscan github.com >> ~/.ssh/known_hosts
vim ~/.ssh/config
Добавить ключ для доступа в GitLab на ВМ:

Host github.com
  HostName github.com
  User git
  IdentityFile ~/.ssh/github_actions
  IdentitiesOnly yes
Проверить соединение:

ssh -T git@github.com
Не забыть прописать публичную часть SSH ключа из пары, которая указана в GitHub в authorized_keys

Открыть порт:

sudo ufw enable
sudo ufw allow OpenSSH  
sudo ufw allow 8000       
sudo ufw status
Создать дирректорию и склонировать репозиторий:

mkdir code
cd code
git clone https://github.com/alevch10/amplitude_downloader.git
cd
Настроить systemd:

touch /etc/systemd/system/fastapi-app.service
vim /etc/systemd/system/fastapi-app.service
Вставить:

[Unit]
Description=FastAPI app for amplitude-downloader
After=network.target

[Service]
User=root
WorkingDirectory=/root/code/amplitude_downloader
ExecStart=/usr/bin/env poetry run uvicorn main:app --host 0.0.0.0 --port 8000
Restart=always
Environment="PATH=/root/.local/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"

[Install]
WantedBy=multi-user.target
sudo systemctl daemon-reload
sudo systemctl restart fastapi-app

```