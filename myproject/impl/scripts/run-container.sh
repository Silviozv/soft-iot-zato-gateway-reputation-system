#!/bin/bash

# Common options
set -e
set -x
set -o pipefail
shopt -s compat31

# Find our current directory
CURDIR="${BASH_SOURCE[0]}";RL="readlink";([[ `uname -s`=='Darwin' ]] || RL="$RL -f")
while([ -h "${CURDIR}" ]) do CURDIR=`$RL "${CURDIR}"`; done
N="/dev/null";pushd .>$N;cd `dirname ${CURDIR}`>$N;CURDIR=`pwd`;popd>$N

# What environment this is
export env_name=myproject

# What password to use when logging in to the dashboard
export zato_password=123456

#variavel para controlar se salva dados do dispositivo ou nao
#precisa no env passar como q eh Zato_nome do env
export SAVE_DATA_ENABLED=True

# set dos tempos de coleta e publicação para os novos dispositivos ainda nao configurados
export COLLECTION_TIME=2
export PUBLISH_TIME=6

# indica o tamanho do bloco temporal que será utilizado para realizar a agregação dos dados
export AGGREGATION_WINDOW_MINUTES=10

# indica o tempo de retenção dos dados que estão no banco de dados
# o tempo tem que ser maior do que o tempo de agregação dos dados
# os dados que passam desse tempo de retenção são apagados
export DATA_RETENTION_SECONDS=1200

# How much of the logging details to show, e.g. "-v" or "-vvvvv"
export zato_build_verbosity=${Zato_Build_Verbosity:-""}

# Name the container
export container_name=zato-$env_name

# Absolute path to where to install code in the container
export target=/opt/hot-deploy

# --- Nome da sua imagem ---
export package_address=rhianpablo11/esb-zato-soft-iot:v10
# export package_address=zato-image-base-test-v7

# Absolute path to our source code on host
# ATENÇÃO: Esse script assume que ele está dentro de uma pasta 'bin' ou similar
# e que seus arquivos de config estão duas pastas acima.
export host_root_dir=`readlink -f $CURDIR/../../`

# Directory on host pointing to the git clone with our project
export zato_project_root=$host_root_dir

# Our enmasse file to use
export enmasse_file=enmasse.yaml
export enmasse_file_full_path=$host_root_dir/config/enmasse/$enmasse_file

#onde os arquivos de configuração irao
export host_data_dir_archives_to_send='../../impl/src/archives/'
export container_data_dir=/home/ubuntu/mapping_archives/devices_config/


#========================================

# Variáveis adicionadas

export GATEWAY_REAL_IP="127.0.0.1"   # Tem que modificar 

export TANGLE_API_IP="$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' api-tangle)"
export TANGLE_API_PORT="3000"


export ZMQ_IP="$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' zmq-tangle)"
export ZMQ_PORT='5556'

export NODE_TYPE="4"
export HONESTY_RATE="50"
export GROUP="cloud/c1"

#========================================


# Directory for auto-generated environment variables
mkdir -p $host_root_dir/config/auto-generated

# Populate environment variables for the server
echo '[env]'                               > $host_root_dir/config/auto-generated/env.ini
echo My_API_Password_1=$My_API_Password_1 >> $host_root_dir/config/auto-generated/env.ini
echo My_API_Password_2=$My_API_Password_2 >> $host_root_dir/config/auto-generated/env.ini
echo Zato_Project_Root=$target/$env_name  >> $host_root_dir/config/auto-generated/env.ini

# Log what we're about to do
echo Starting container $container_name

# Remove container antigo se existir
docker rm --force $container_name || true &&

# --- ALTERAÇÃO 2: Removido o '--pull=always' ---
docker run                                                         \
    --name $container_name                                         \
    --restart unless-stopped                                       \
    -p 22022:22                                                    \
    -p 8183:8183                                                   \
    -p 8184:8184                                                   \
    -p 11223:11223                                                 \
    -p 11225:11225                                                 \
    -p 33033:3000                                                  \
    -p 35672:15672                                                 \
    -p 1883:1883                                                   \
    -p 9001:9001                                                   \
    -e Zato_Dashboard_Password=$zato_password                      \
    -e ZATO_SSH_PASSWORD=$zato_password                            \
    -e Zato_IDE_Password=$zato_password                            \
    -e Zato_Log_Env_Details=true                                   \
    -e Zato_COLLECTION_TIME=$COLLECTION_TIME                       \
    -e Zato_PUBLISH_TIME=$PUBLISH_TIME                             \
    -e Zato_AGGREGATION_WINDOW_MINUTES=$AGGREGATION_WINDOW_MINUTES \
    -e Zato_Build_Verbosity="$zato_build_verbosity"                \
    -e Zato_SAVE_DATA_ENABLED=$SAVE_DATA_ENABLED                   \
    -e Zato_DATA_RETENTION_SECONDS=$DATA_RETENTION_SECONDS         \
    -e Zato_GATEWAY_REAL_IP=$GATEWAY_REAL_IP                       \
    -e Zato_TANGLE_API_IP=$TANGLE_API_IP                           \
    -e Zato_TANGLE_API_PORT=$TANGLE_API_PORT                       \
    -e Zato_ZMQ_IP=$ZMQ_IP                                         \
    -e Zato_ZMQ_PORT=$ZMQ_PORT                                     \
    -e Zato_NODE_TYPE=$NODE_TYPE                                   \
    -e Zato_HONESTY_RATE=$HONESTY_RATE                             \
    -e Zato_GROUP=$GROUP                                           \
    --mount type=bind,source=$zato_project_root,target=$target/$env_name,readonly \
    --mount type=bind,source=$enmasse_file_full_path,target=$target/enmasse/enmasse.yaml,readonly \
    --mount type=bind,source=$host_root_dir/config/auto-generated/env.ini,target=$target/enmasse/env.ini,readonly \
    --mount type=bind,source=$host_root_dir/config/python-reqs/requirements.txt,target=$target/python-reqs/requirements.txt,readonly \
    --mount type=bind,source=$host_data_dir_archives_to_send,target=$container_data_dir \
    $package_address
