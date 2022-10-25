# 关于AbaseCode
AbaseCode OpenCode是一套开源合集。包括基础包、工具包、安全包、token包、支付包、excel包等。

开源项目的组件做到开箱即用，方便更多的开发者节省重复的工作，更专注于业务逻辑代码编写。

我是Jon，一名全栈开发者，专注于学习和传播技术知识。希望这些工具包能够帮上你，欢迎有的朋友加入这个开源项目。

project homepage : https://abasecode.com

project github : https://github.com/abasecode

Jon's blog : https://jon.wiki

e-mail: ijonso123@gmail.com

# 关于 abasecode-base-es
一个基于elasticsearch-java version 7.17的客户端，使用最新的java-api版本。


# 快速开始
## 步骤 1: 添加依赖
``` xml
<dependency>
    <groupId>com.abasecode.opencode</groupId>
    <artifactId>abasecode-base-es</artifactId>
    <version>1.0.0</version>
</dependency>
```

## 步骤 2: 配置 application.yaml
如下:
``` yaml
app:
  elasticsearch:
    uris:
      - http://192.168.3.230:9200
    username: elastic
    password: Es789456
```
uris 可以配置多个uri。必须以http:// 或 https://开头。
## 步骤 3: 添加注解
```java
@EnableCodeEs
public class App {
    public static void main(String[] args) {
        SpringApplication.run(App.class,args);
    }
}
```

## 步骤 4: 完成