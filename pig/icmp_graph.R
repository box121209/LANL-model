
icmp <- read.csv(pipe("cat icmp_edges.txt | tr -d '()' | cut -d',' -f1,2"))

library(igraph)

g <- graph.edgelist(as.matrix(icmp))

cc <- clusters(g)
cc$no
cc$csize

giant <- induced.subgraph(g, V(g)[cc$membership==1])


V(giant)$size <- log(1 + degree(giant))
V(giant)$color <- 'red'
V(giant)$label <- ""
#V(giant)$label.cex <- 0.05*V(giant)$size
E(giant)$arrow.mode <- '-'

plot(giant)






