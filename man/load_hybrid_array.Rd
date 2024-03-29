% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/hybrid_generics.R
\name{load_hybrid_array}
\alias{load_hybrid_array}
\title{Load Hybrid Array Stored in Hard Disk}
\usage{
load_hybrid_array(path)
}
\arguments{
\item{path}{directory where hybrid array is stored}
}
\description{
Load Hybrid Array Stored in Hard Disk
}
\examples{

## Create an array
data <- rnorm(1e5)
x <- hybrid_array(data, c(100, 100, 10))

# save to disk
x$swap_out()
path = x$file_location

# Load from disk
y = load_hybrid_array(path)

# check
range(y[] - x[]) # should be 0,0

}
