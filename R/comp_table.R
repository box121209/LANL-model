
dat <- read.csv("comp_table.txt", header=FALSE) 
n <- nrow(dat)
cmp <- sapply(1:n, function(i) if(dat[i,1]=="") dat[i,4] else dat[i,1])
df <- data.frame(cmp, dat[,c(2,3,5,6)])
df[is.na(df)] <- 0
names(df) <- c("cmp","outflow","outbyte","inflow","inbyte")
attach(df)


plot(inflow, outflow, log='xy', cex=0.3, col='blue', frame.plot=0)
abline(0,1, col='grey')


plot(inbyte, outbyte, log='xy', cex=0.3, col='blue', frame.plot=0)
abline(0,1, col='grey')
