# Whale v1.0

Whale, a novel mechanism for efficient serialization and transferring stream data between processes for one-to-many communication in DSPSs. 
Whale serializes messages before they are encapsulated into tuples in order to reduce serialize computation and transfers only once to each process to greatly
improve the communication efficiency.We implement Whale on top of Apache Storm. Experiment results show that Whale significantly improves the performance of the existing design in terms of system
throughput and process latency.
## License

Whale is released under the [Apache 2 license](http://www.apache.org/licenses/LICENSE-2.0.html).

## Project Committers
* HanHua Chen([@chen](chen@hust.edu.cn))
* Fan Zhang([@zhangf](zhangf@hust.edu.cn))
* Jie Tan ([@tjmaster](https://tjcug.github.io/))
* Yonghui Wang([@wyh](https://github.com/WYonghui/))
* HaoPeng Jie([@jhp](https://github.com/jessezax/))

## Author and Copyright

Whale is developed in Cluster and Grid Computing Lab, Services Computing Technology and System Lab, Big Data Technology and System Lab, School of Computer Science and Technology, Huazhong University of Science and Technology, Wuhan, China by Hanhua Chen (chen@hust.edu.cn), Fan Zhang(zhangf@hust.edu.cn), Hai Jin (hjin@hust.edu.cn), Jie Tan(tjmaster@hust.edu.cn)
Yonghui Wang(wangyonghui@hust.edu.cn),HaoPeng Jie(jhp@hust.edu.cn)

Copyright (C) 2017, [STCS & CGCL](http://grid.hust.edu.cn/) and [Huazhong University of Science and Technology](http://www.hust.edu.cn).


