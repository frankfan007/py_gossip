###################################
# Standard node evolution chart:
#
# 	node_1	node_2	node_3
#	20	40	30
#	25	40	25
#	32.5	32.5	25
#	30	30	30
#
# Cycle number is assumedly in order
# Default data type is data.frame
####################################


# To read CSV file
# Can adapt to different form of data
# Input: CSV file
# Output: standard node evolution chart
usr_read <- function(resource="fd_with_id.csv", nodeID=TRUE, cycleNum=FALSE, transpose=TRUE) {
	temp <- data.frame()
	if (transpose) {
		if (nodeID && cycleNum) {
			temp <- read.csv(resource, sep=",", header=TRUE, row.names=1)
		} else if (nodeID) {
			temp <- read.csv(resource, sep=",", header=FALSE, row.names=1)
		} else if (cycleNum) {
			temp <- read.csv(resource, sep=",", header=TRUE, row.names=NULL)
			rownames(temp) <- gen_name(num=nrow(temp))
		} else {
			temp <- read.csv(resource, sep=",", header=FALSE, row.names=NULL)
			rownames(temp) <- gen_name(num=nrow(temp))
		}
		temp <- as.data.frame(t(temp))
	} else {
		if (nodeID && cycleNum) {
			temp <- read.csv(resource, sep=",", header=TRUE, row.names=1)
		} else if (nodeID) {
			temp <- read.csv(resource, sep=",", header=FALSE, row.names=1)
		} else if (cycleNum) {
			temp <- read.csv(resource, sep=",", header=TRUE, row.names=NULL)
		} else {
			temp <- read.csv(resource, sep=",", header=FALSE, row.names=NULL)
		}
	}
	return(temp)
}

# Generate name using format: name_#
# Output: Vector of char
gen_name <- function(preffix="nodeID", num=7) {
	temp <- vector()
	for (i in 1:num) {
		temp <- c(temp, paste(preffix, i, sep="_"))
	}
	return(temp)
}


#calculate variance for every row
# Input: data.frame
# Output: a vector of the variance of each row
var_vector <- function(data_frame, print=FALSE) {
	m <- as.matrix(data_frame)
	vec <- vector()
	for (i in 1:nrow(data_frame)) {
		vec[i] <- var(m[i,])
		#cat(vec,"\n")
	}
	if (print==TRUE) {
		vec
	}
	return(vec)
}

# Take a list of data.frame, could be the run result ofdifferent topology of network
# Input: list(data.frame)
# Output: data.frame() of variance in each cycle and topology
var_frame <- function(frame_list, name_list) {
	lst <- list()
	if(missing(name_list)) {
		name_list <- c(as.character(1:length(frame_list)))
	}
	for (i in 1:length(frame_list)) {
		lst[[i]] <- c(var_vector(frame_list[[i]]))
	}
	fra <- do.call(cbind.data.frame, lst)
	colnames(fra) <- name_list
	return(fra)
}

# append cycle column and melt the data.frame for plotting
# Input: data.frame
# Output: melted data.frame
cycle_and_melt <- function(data_frame) {
	temp <- data_frame
	temp$cycle=1:nrow(temp)
	melted<-melt(temp, id="cycle")
	return(melted)
}

# input: standard node evolution chart
# output: convergence graph
plot_var <- function(data_frame) {
	melted <- cycle_and_melt(data_frame)
	p <- ggplot(melted, aes(x=cycle, y=value, colour=variable))
	p <- p + geom_line()
	return(p)
}

# input: standard node evolution chart, with nodeID
# output: scatter plot
# modify geom_point() to adjust appearance
plot_convergence <- function(data_frame) {
	melted <- cycle_and_melt(data_frame)
	p <- ggplot(melted, aes(x=cycle, y=value, colour=variable))
	p <- p + geom_point(alpha=I(0.7), size=10)
	p <- p + xlab("# of Cycle") + ylab("stateValue")
	
	return(p)
}
