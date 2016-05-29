
sqlite3 netflow.db

------------------------------------------
-- getting started:

CREATE TABLE flows(
       time INT,
       duration INT,
       src VARCHAR(10),
       src_port VARCHAR(6),
       dst VARCHAR(10),
       dst_port VARCHAR(6),
       proto INT,
       packets INT,
       bytes INT
);
.separator ","
.import flows.csv flows
.schema flows

------------------------------------------
-- initial EDA...

-- how many flows? 
select count(*) from flows;
-- 129977412

-- protocol distribution?
SELECT proto,count(proto) FROM flows GROUP BY proto;

-- what is protocol 41? IPv6 encapsulation...
SELECT * FROM flows WHERE proto=41;
SELECT src,dst FROM flows WHERE proto=41;
-- note that src_port always = dst_port

-- time range:
SELECT min(time),max(time) FROM flows;
-- only 36 days?

-- how many distinct computers?
SELECT count(*) FROM 
      (SELECT distinct(src) FROM flows 
	UNION 
	SELECT distinct(dst) FROM flows);
-- 12027
-- we should be able to cluster and visualise these...

-- distribution of nr events/second:

SELECT count(*) FROM flows WHERE time=1;
SELECT ne,count(ne) FROM 
       (SELECT count(time) AS ne FROM flows GROUP BY time) 
GROUP BY ne ORDER BY ne;

-- distribution of nr packets:
SELECT packets,count(packets) FROM flows GROUP BY packets ORDER BY packets;

SELECT * FROM flows WHERE packets>2000000; 
-- all TCP (proto=6)

-- who uses ICMP (proto=1)?
SELECT count(distinct src),count(distinct dst) FROM flows;
-- 11154,8711
SELECT count(distinct src),count(distinct dst) FROM flows WHERE proto=1; 
-- 5462,7067

-- distribution of ICMP count by source -- clearly a mixture of human-
-- and machine-generated traffic?

.mode csv
.output 'icmp_freq.csv'
SELECT n_icmp,count(n_icmp) FROM 
       (SELECT count(src) AS n_icmp FROM flows WHERE proto=1 GROUP BY src)
GROUP BY n_icmp ORDER BY n_icmp; 
 .output 'stdout'

------------------------------------------
-- server port analysis


-- distribution of well-known ports:
select dst_port,count(dst_port) from flows where dst_port NOT LIKE
"N%" group by dst_port order by CAST(dst_port AS INTEGER);

-- or:
.mode csv
.output 'dst_port_freq.csv'
SELECT dst_port,count(dst_port) 
FROM flows 
WHERE dst_port NOT LIKE "N%" 
GROUP BY dst_port 
ORDER BY count(dst_port);
.output 'stdout'

CREATE TABLE dst_port_freq(
       dst_port VARCHAR(6),
       nflows INT
);
.separator ","
.import dst_port_freq.csv dst_port_freq
.schema dst_port_freq

select * from dst_port_freq;


-- focus attention on 25 port numbers with >9000 events: 
Port (frequency)
22 (652179)	SSH
53 (13357)	DNS
80 (4811435)	HTTP
88 (2019214)	Kerberos authentication system
111 (237231)	Open Network Computing (ONC) Remote Procedure Call
    		     	(RPC) is a widely deployed remote procedure
			call system. Microsoft supplies an implementation for
			Windows in their Microsoft Windows Services
			for UNIX product; in addition, a number of
			third-party implementation of ONC RPC for Windows exist.
123 (361530)	Network Time Protocol (NTP), used for time synchronization
135 (1002218)	Microsoft EPMAP (End Point Mapper), also known as 
    			DCE/RPC Locator service, used to remotely
			manage services including DHCP server, DNS
			server and WINS. 
			Also used by DCOM
137 (2562961)	NetBIOS Name Service
138 (368393)	NetBIOS Datagram Service
139 (2123849)	NetBIOS Session Service
161 (181290)	SNMP. Managing devices on IP networks". Devices that
    		      	typically support SNMP include routers,
			switches, servers, workstations, printers,
			modem racks and more.
389 (3951978)	Lightweight Directory Access Protocol (LDAP)
427 (12614)	Service Location Protocol (SLP). SLP is used by
    			devices to announce services on a local
			network. Each service must have a URL that is
			used to locate the service.
443 (898289)	HTTPS
445 (25912435)Microsoft-DS Active Directory, Windows shares.
1094 (16016)	?
1178 (9219)	?
1214 (10826)	Kazaa Media Desktop:  peer-to-peer file sharing
     		      	application; commonly used to exchange MP3
			music files and other file types, such as
			videos, applications, and documents over the Internet.
1241 (58628)	Nessus Security Scanner. World's most popular
     		       	vulnerability scanner.
1433 (126420)	Microsoft SQL Server database management system
     			(MSSQL) server.
2049 (360818)	Network File System
3306 (36303)	MySQL database system.
6002 (16151)	[Ports 6000, 6001:] X11 —
     		       	used between an X client and server over the network.
7002 (16232)	Default for BEA WebLogic Server's HTTPS server, 
     			though often changed during installation.
8080 (15717) 	HTTP alternative—commonly used for Web proxy and 
     		     	caching server, or for running a Web server as a non-root user.


-- for example, let's take a look at Nessus on port 1241:
select count(distinct(src)),count(distinct(dst)) from flows where dst_port=1241;
-- 55,1397
select count(distinct(src)),count(distinct(dst)) from flows where src_port=1241;
-- 1482,60
-- is 55 a subset of 60?
select count(*) from
       (select distinct(dst) from flows where src_port=1241
       union
       select distinct(src) from flows where dst_port=1241);
-- 75
-- so are these 75 sysadmin machines?

-- web servers?
select count(distinct dst) from flows where dst_port=80 OR dst_port=443;

------------------------------------------
-- build a table of interesting ports for use below:

CREATE TABLE server_ports(dst_port TEXT PRIMARY KEY);
INSERT INTO server_ports
       SELECT dst_port FROM
       	      (SELECT dst_port FROM dst_port_freq WHERE nflows>1500)
       ORDER BY CAST(dst_port AS INTEGER);


------------------------------------------
-- further reference tables for building feature vectors below:

CREATE TABLE boxes(
       box VARCHAR(10) PRIMARY KEY
);
INSERT INTO boxes
       SELECT distinct(src) FROM flows 
       UNION 
       SELECT distinct(dst) FROM flows;

CREATE TABLE servers(
       box VARCHAR(10),
       dst_port VARCHAR(6),
       PRIMARY KEY (box, dst_port)
);
INSERT INTO servers
       SELECT * FROM boxes CROSS JOIN server_ports;

CREATE TABLE protocols(
       proto INT PRIMARY KEY
);
INSERT INTO protocols
       SELECT DISTINCT proto FROM flows;

CREATE TABLE computers(
       box VARCHAR(10),
       proto INT,
       PRIMARY KEY (box, proto)
);
INSERT INTO computers
       SELECT * FROM boxes CROSS JOIN protocols;


------------------------------------------
-- protocol flow features:

CREATE TABLE proto_features_in(
       box VARCHAR(10),
       proto INT,
       inflows INT,
       inbytesmean FLOAT,
       indurationmean FLOAT,
       PRIMARY KEY (box, proto)
);
INSERT INTO proto_features_in
       SELECT dst, proto, count(*), avg(bytes), avg(duration)
       FROM flows
       GROUP BY dst,proto;
INSERT INTO proto_features_in
       SELECT box,proto,0,0,0 FROM computers
       EXCEPT 
       SELECT box,proto,0,0,0 FROM proto_features_in; 

CREATE TABLE proto_features_out(
       box VARCHAR(10),
       proto INT,
       outflows INT,
       outbytesmean FLOAT,
       outdurationmean FLOAT,
       PRIMARY KEY (box, proto)
);
INSERT INTO proto_features_out
       SELECT src, proto, count(*), avg(bytes), avg(duration)
       FROM flows
       GROUP BY src,proto;
INSERT INTO proto_features_out
       SELECT box,proto,0,0,0 FROM computers
       EXCEPT 
       SELECT box,proto,0,0,0 FROM proto_features_out; 

CREATE TABLE proto_features AS
       SELECT *
       FROM proto_features_in NATURAL JOIN proto_features_out;
DROP TABLE proto_features_in;
DROP TABLE proto_features_out;

.mode csv
.output 'proto_features.csv'
SELECT * FROM proto_features;
.output 'stdout'

------------------------------------------
-- port features:

CREATE TABLE server_flows AS
       SELECT * FROM flows NATURAL JOIN server_ports;

CREATE TABLE port_features_in(
       box VARCHAR(10),
       dst_port TEXT,
       inflows INT,
       inbytesmean FLOAT,
       indurationmean FLOAT,
       PRIMARY KEY (box,dst_port)
);
INSERT INTO port_features_in
       SELECT dst,dst_port,count(*),avg(bytes),avg(duration) 
       FROM server_flows
       GROUP BY dst,dst_port;
INSERT INTO port_features_in
       select box,dst_port,0,0,0 from servers
       EXCEPT 
       select box,dst_port,0,0,0 from port_features_in; 

CREATE TABLE port_features_out(
       box VARCHAR(10),
       dst_port TEXT,
       outflows INT,
       outbytesmean FLOAT,
       outdurationmean FLOAT,
       PRIMARY KEY (box,dst_port)
);
INSERT INTO port_features_out
       SELECT src, dst_port, count(*), avg(bytes), avg(duration) 
       FROM server_flows
       GROUP BY src, dst_port;
INSERT INTO port_features_out
       select box,dst_port,0,0,0 from servers
       EXCEPT 
       select box,dst_port,0,0,0 from port_features_out; 

CREATE TABLE port_features AS
       SELECT *
       FROM port_features_in NATURAL JOIN port_features_out;
DROP TABLE port_features_in;
DROP TABLE port_features_out;

.mode csv
.output 'port_features_2.csv'
SELECT * FROM port_features;
.output 'stdout'

------------------------------------------


