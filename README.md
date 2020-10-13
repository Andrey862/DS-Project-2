# Project 2
## How to launch
### Naming server
>docker run -d -p 8800-8810:8800-8810 andrey2/namingserver:1.1
### Data node
>docker run -d --env NN=\<ip of your NS here\> -p 8800-8810:8800-8810 andrey2/datanode:1.0 
### Clinet
git clone https://github.com/Andrey862/DS-Project-2 <br/>
cd Client <br/>
python client.py
## Client commands
```
<NS_ip>
ls .
mkdir Foder_name
rm File_name
rm Folder_name
write File_name
read File_name
ls . -r
```

## Architecture
![Architecture](./img/DS_P2.jpg)
## Contribution
Andrey | Magomed
------|------
Design | Naming server
Data Node | Client
Client | 