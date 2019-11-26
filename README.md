# Postman
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
  4. 为调试工具设置lauch.json，加入以下JVM参数：
      * -Djava.library.path=`源码目录`
      * -Djasypt.encryptor.password=`请向管理员询问密码，或自己修改配置`

Eclipse (TODO)

`注意` Postman连接数据上下游时，请事先确保相应对接环境已准备好。
