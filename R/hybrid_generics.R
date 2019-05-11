
#' @export
`[.HybridArray` <- function(x, ..., drop = TRUE){

  if(!x$initialized){
    return(NA)
  }
  nargs  =...length()

  ###
  is_formula = FALSE
  if(nargs > 0 && x$is_namedarray){
    is_formula = tryCatch({
      'formula' %in% class(...elt(1))
    }, error = function(e){
      FALSE
    })
  }
  if(is_formula){
    # tidy evaluation
    parent_env = new.env(parent = parent.frame())
    fs = list(...)
    dimnames = x$get_dimnames()
    nms = names(dimnames)
    roi = lapply(dimnames, function(x){ rep(TRUE, length(x)) })
    for(f in fs){
      target = unlist(as.character(f[[2]]))[1]
      if(target %in% nms){
        sel = eval(f[[3]], envir = dimnames, enclos = parent_env)
        idx = which(nms == target)
        roi[[idx]] = sel & roi[[idx]]
      }
    }
    roi = lapply(roi, which)
    names(roi) = NULL
    re = do.call(x$get_data, roi)
  }else{
    if(nargs > 1 && x$ndims != nargs){
      stop('incorrect number of dimensions')
    }
    re = x$get_data(...)
  }

  if(drop){
    drop(re)
  }else{
    re
  }
}

#' @export
`[<-.HybridArray` <- function(x, ..., value){
  if(x$initialized){
    x$alter_data(value = value, ...)
  }else{
    if(missing(..1)){
      x$init_data(value, dim(value), dimnames(value))
    }else{
      x$init_data(value, dim(value), dimnames(value), ..., partial = TRUE)
    }
  }
  x
}

#' @title Create a Hybrid Array
#'
#' @description These functions create hybrid array instances for fast read/write process for
#' arrays with three or more dimensions. When the data is too large for RAM,
#' it's recommended to partition and store data on the local hard disks. Hybrid
#' array partitions the data along one of its dimensions.
#'
#' @param data array or an atomic element
#' @param dim dimension of data
#' @param dimnames \code{NULL} or named list of data dimensions
#' @param path path to store array
#' @param partition_index which dimension to create partition
#'
#' @details
#' When the array is too large for RAM to handle, use \code{hybrid_array_partial}.
#' For example, a 1000 x 1000 x 100 x 100 array could be ~ 80GB which could be
#' too big for a personal laptop to handle in RAM. To solve this problem, we could
#' generate 1000 sub-arrays with dimension 1 x 1000 x 100 x 100, with each ~ 80 MB.
#' To start, we use \code{hybrid_array_partial(..., which_partition=1)} to claim
#' the first dimension to be the partition index, then push sub-arrays. (see
#' example - "partial data")
#'
#' @examples
#'
#' # ------------ Simple in-memory usage ------------
#' data <- rnorm(1e5)
#' x <- hybrid_array(data, c(100, 100, 10))
#' x[]
#'
#' # ------------ partial data example ------------
#' # generate a 10 x 10 x 3 x 100 array x, but only with partial data
#'
#' # the second partition
#' data = array(rnorm(10000), c(10, 10, 1, 100))
#'
#' # x = array(NA, c(10, 10, 3, 100)); x[,,2,] <- data
#' x = hybrid_array_partial(data, dim = c(10,10,3,100), partition_index = 3, which_partition = 2)
#' x[,,2,]
#'
#' # Add more data
#' x[,,3,] <- data + 1
#'
#' # Check, should be all '1'
#' x[1,1,3,] - x[1,1,2,]
#'
#' \dontrun{
#' # ------------ Hybrid example ~ 800MB data ------------
#' data <- rnorm(1e8)
#' x <- hybrid_array(data, c(100, 100, 100, 100))
#' x$ram_used
#' # save to disk, might take a while to write to disk
#' x$swap_out(); x$ram_used
#' x[1,2,1:10,2]
#' }
#'
#' @export
hybrid_array <- function(data = NA, dim = length(data), dimnames = NULL,
                         path = tempfile(pattern = 'hybridarray'), partition_index = NULL){
  x = HybridArray$new(path = path, read_only = FALSE)
  x$set_dim(dim)
  if(length(partition_index) == 1){
    if(partition_index > 0){
      x$set_partition_index(which.idx = partition_index)
    }else{
      x$use_partition = FALSE
    }
  }
  x$init_data(x = data, dim = dim, dimnames = dimnames)
  x
}

#' @rdname hybrid_array
#' @param which_partition which partition should data be when calling \code{hybrid_array_partial}
#' @export
hybrid_array_partial <- function(
  data, dim, which_partition, partition_index = length(dim),
  dimnames = base::dimnames(data),
  path = tempfile(pattern = 'hybridarray')
){
  if(length(dim) <= 2){
    stop('array dimension less than 3 not supported')
  }
  if(partition_index <= 0 || partition_index > length(dim)){
    stop('partition_index should be a index of dimension')
  }
  if(which_partition <= 0 || which_partition > dim[partition_index]){
    stop('which_partition: incorrect number of dimensions')
  }
  x = HybridArray$new(path = path, read_only = FALSE)
  x$set_dim(dim)
  x$set_partition_index(which.idx = partition_index)

  args = lapply(seq_along(dim), function(x){
    if(x == partition_index){
      which_partition
    }else{
      substitute()
    }
  })

  args = c( alist(x = data, dim = dim, dimnames = dimnames, partial = TRUE), args )

  do.call(x$init_data, args)
  x
}


#' @title Load Hybrid Array Stored in Hard Disk
#' @param path directory where hybrid array is stored
#' @examples
#'
#' ## Create an array
#' data <- rnorm(1e5)
#' x <- hybrid_array(data, c(100, 100, 10))
#'
#' # save to disk
#' x$swap_out()
#' path = x$file_location
#'
#' # Load from disk
#' y = load_hybrid_array(path)
#'
#' # check
#' range(y[] - x[]) # should be 0,0
#'
#' @export
load_hybrid_array <- function(path){
  path = normalizePath(path, mustWork = TRUE)
  HybridArray$new(path = path)
}

# self = load_hybrid_array('/var/folders/rh/4bkfl5z50wgbbjd85xvc695c0000gn/T//RtmpAbsJ6B/hybridarray37655e18234b')
# self[]
# self$get_dim()
# self$alter_data(13, 1,1,1)
#
# private=self$.__enclos_env__$private
# self[drop=F]
# self$stack_print()
# self$swap_out()
#
#
# private$locked=T
# self$reset_partition(partition = 1, replace = NA)
# self$saved_partitions
