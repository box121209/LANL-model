
protofeat <- read.csv("data/proto_features.csv", header=FALSE)
names(protofeat) <- 
  c("box",
    "proto",
    "inflows",
    "inbytesav",
    "indurationav",
    "outflows",
    "outbytesav",
    "outdurationav")
portfeat <- read.csv("data/port_features_2.csv", header=FALSE)
names(portfeat) <- 
  c("box",
    "dstport",
    "inflows",
    "inbytesav",
    "indurationav",
    "outflows",
    "outbytesav",
    "outdurationav")

boxes <- sort(unique(portfeat$box))
ports <- sort(unique(portfeat$dstport))
protos <- sort(unique(protofeat$proto))

# nr flows by box:
tmp <- protofeat$inflows + protofeat$outflows
nflow <- aggregate(tmp, list(protofeat$box), sum)
protofeat <- merge(protofeat, nflow, by.x="box", by.y="Group.1")
portfeat <- merge(portfeat, nflow, by.x="box", by.y="Group.1")
names(protofeat)[9] <- "nflow"
names(portfeat)[9] <- "nflow"

# nr bytes by box:
tmp <- protofeat$inbytesav*protofeat$inflows + protofeat$outbytesav*protofeat$outflows
nbyte <- aggregate(tmp, list(protofeat$box), sum)
protofeat <- merge(protofeat, nbyte, by.x="box", by.y="Group.1")
portfeat <- merge(portfeat, nbyte, by.x="box", by.y="Group.1")
names(protofeat)[10] <- "nbyte"
names(portfeat)[10] <- "nbyte"

# nr seconds by box:
tmp <- protofeat$indurationav*protofeat$inflows + protofeat$outdurationav*protofeat$outflows
nsec <- aggregate(tmp, list(protofeat$box), sum)
protofeat <- merge(protofeat, nsec, by.x="box", by.y="Group.1")
portfeat <- merge(portfeat, nsec, by.x="box", by.y="Group.1")
names(protofeat)[11] <- "nsec"
names(portfeat)[11] <- "nsec"

# sort these data frames in order of box id:
protofeat <- protofeat[order(protofeat$box, protofeat$proto),]
portfeat <- portfeat[order(portfeat$box, portfeat$dstport),]

