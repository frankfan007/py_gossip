source("/Users/guoger/academia/RMSW/R/util/util.r")

vis_exp <- function(dir=".", name="foo") {
	exp <- read_node_raw(dir)
	exp.plot <- plot_raw(exp, title=name)
	exp.plot <- exp.plot + ggtitle(name)
	exp.plot
	return(exp.plot)
}

vis_sim <- function(src, name="foo") {
	sim <- usr_read(src, FALSE, FALSE, FALSE)
	sim.plot <- plot_convergence(sim)
	sim.plot <- sim.plot + ggtitle(name)
	sim.plot
	return(sim.plot)
}
