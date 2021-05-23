# README

author ： GCC

​		基于Apache-Nifi进行开发的自定义Processor，主要作用即是通过自定义的逻辑配置，将json数据进行分发，在Nifi中的表现方式为一个Processor，可进行动态的追加Relationship，且每个Relationship都对应一个配置好的规则，当前方数据汇入该组件，则根据对应的配置将数据分发到不同的Relationship中。

## Logic逻辑配置的定义

这是作者自定义的一组dsl

样例：

```json
{
  "logic":"&&",
  "fields":[
     {
	   "field":"age",
	   "value":[23]
	 },
	 {
	   "field":"sex",
	   "value":["女"]	
	 }
  ]
}
```

### 构成方式

​	整个json由 **logic**和**fields**字段构成，logic代表的是逻辑，fields代表的具体的规则。

### Logic

​	logic的枚举有：

| 枚举 | 意义                    |
| ---- | ----------------------- |
| &&   | 且的逻辑，即逻辑种的AND |
| \|\| | 或的逻辑，即OR          |

​	这里的logic，主要的作用域是fields中的字段，例如：当logic的值为 “&&”时，若fields数组中存在多个json实体，那么这些json实体必须是且的关系。

### Fields

​	这里fields字段的值是个数组，数组中每一个元素可作为一条规则，规则的样例：

```json
{
  "field":"",
  "value"[]
}
```

​	一个规则由field和value来表述，field代表的是待分发数据中的字段，value代表的是该字段应该满足哪些值。

​	value中的值关系是满足任意一个即可。

​	fields数组中规则的关系受logic字段的逻辑限制。

### 样例

例如一个待判断的json数据样例是：

```json
[
{
  "name":"zhangsan1",
  "age":12,
  "sex":"男"
},
{
  "name":"zhangsan2",
  "age":8,
  "sex":"女"  
},
{
  "name":"zhangsan3",
  "age":22,
  "sex":"男"    
},
{
  "name":"zhangsan4",
  "age":22,
  "sex":"女"    
}
]
```

我们要筛选出名字是zhangsan2的，对应的规则为：

```json
{
  "logic":"&&",
  "fields":[
     {
	   "field":"name",
	   "value":["zhangsan2"]
	 }
  ]
}
```

若要筛选出年龄是22，性别是女性的：

```json
{
  "logic":"&&",
  "fields":[
     {
	   "field":"age",
	   "value":[22]
	 },
	 {
	   "field":"sex",
	   "value":["女"]	
	 }
  ]
}
```

若需筛选出名字是zhangsan1或者年龄在22岁的：

```json
{
  "logic":"||",
  "fields":[
     {
	   "field":"age",
	   "value":[22]
	 },
	 {
	   "field":"name",
	   "value":["zhangsan1"]	
	 }
  ]
}
```



## 组件的用法

​	1、将该代码打包生成nar包程序

​	2、将nar包放入Nifi安装目录的lib目录下

​	3、重启Nifi程序

​	4、在Processor导航中即可找到名为 JsonDistributeProcessor的处理器，引入处理器

​	5、在配置页面，追加PropertyDescriptor，每追加一个PropertyDescriptor，处理器即会生成一个与其对应的Relationship分发管道，在追加的PropertyDescriptor中填入logic表达式，便可进行数据的分发。

​	