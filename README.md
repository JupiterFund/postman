# Postman

[![Gitpod Ready-to-Code](https://img.shields.io/badge/Gitpod-Ready--to--Code-blue?logo=gitpod)](https://gitpod.io/#https://github.com/JupiterFund/postman) 
[![Build Status](https://travis-ci.com/JupiterFund/postman.svg?branch=master)](https://travis-ci.com/JupiterFund/postman)
[![Docker Pulls](https://img.shields.io/docker/pulls/jupiterfund/postman.svg)](https://hub.docker.com/r/jupiterfund/postman/)

Postman是JupiterFund团队自主开发的金融市场行情数据落地程序

目前支持的上游数据接口有：
* 宏汇TDF Remote API
* QTS实时行情系统
* 期货CTP接口

下游输出：
* Kafka
* Redis


## 如何使用

#### Docker

使用postman镜像

```bash
# 使用默认配置运行postman
docker run -d -e JASYPT_ENCRYPTOR_PASSWORD=${密码} jupiterfund/postman

# 使用自定义的配置文件，运行postman
docker run -d -v ${配置文件}:/app/application.yml -e SPRING_CONFIG_LOCATION=file:/app/application.yml -e JASYPT_ENCRYPTOR_PASSWORD=${密码} jupiterfund/postman
```

使用一般java镜像

```bash
# 下载最新版本
git clone https://github.com/JupiterFund/postman
cd postman
# 编译构建
gradle clean copy build bootJar
java -jar postman.jar
# 在build/distribution目录里找到编译结果并解压缩
docker run -d openjdk:8-jdk  java -jar -Djava.library.path=/app -Djasypt.encryptor.password=*** postman.jar
# 按个人本地环境设置需要连接的kafka或redis网段
```

#### 本地开发

Visual Studio Code
  1. 安装准备Java运行环境和调试工具
  2. 打开下载的源代码目录
  3. 按需要调整.project，.classpath
  4. 为调试工具设置lauch.json，加入以下环境变量：
      * LD_LIBRARY_PATH: `源码目录`
      * JASYPT_ENCRYPTOR_PASSWORD: `请向管理员询问密码，或自己修改配置`
      * TZ: "Asia/Shanghai"

Eclipse (TODO)

`注意` Postman连接数据上下游时，请事先确保相应对接环境已准备好。
