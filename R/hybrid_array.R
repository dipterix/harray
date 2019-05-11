.global = new.env(parent = emptyenv())
.global$arrays = list()
.global$array_size = function(individual = FALSE){
  if(individual){
    sapply(.global$arrays, lobstr::obj_size)
  }else{
    lobstr::obj_size(.global$arrays)
  }
}
.global$ram_limit = 1024^3

#' @importFrom R6 R6Class
HybridArray <- R6::R6Class(
  classname = 'HybridArray',
  portable = TRUE,
  cloneable = FALSE,
  private = list(
    path = NULL,
    alt_path = NULL,
    attributes = NULL,
    meta_path = NULL,
    partition = 0,
    index = 0,
    data_env = NULL,
    dim = NULL,
    dimnames = NULL,
    locked = FALSE,
    length = 0,
    new_file = TRUE,
    n = 0,
    cached = NULL,
    file_format = NULL,
    write_table = function(x, file, ...){
      switch (
        private$file_format,
        'fst' = {
          write_table_fst(x, file, ...)
        },
        'csv' = {
          write_table_dt(x, file, ...)
        },{
          stop('file format `', private$file_format, '` not supported')
        }
      )
    },
    load_table = function(file, columns = NULL){
      switch (
        private$file_format,
        'fst' = {
          load_table_fst(file, columns)
        },
        'csv' = {
          load_table_dt(file, columns)
        },{
          stop('file format `', private$file_format, '` not supported')
        }
      )
    }
  ),
  public = list(
    use_partition = TRUE,
    ram_limit = Inf,
    finalize = function(){
      if(!private$locked){
        self$swap_out()
      }
      unlink(private$alt_path, recursive = TRUE, force = TRUE)
    },
    reset_changes = function(){
      private$n = 0
      private$data_env= list()
      private$cached = NULL
      if(private$locked){
        unlink(private$alt_path, recursive = TRUE, force = TRUE)
        private$alt_path = tempfile('junk')
      }
    },
    remove_partition = function(partition, replace = NA, force = FALSE, flush = TRUE){
      if(!force && private$locked){
        stop('Data is locked. Use $reset_partition(force=TRUE) to reset partition.')
      }
      if(length(partition) > 1){
        res = sapply(partition, self$reset_partition, replace = replace, force = force, flush = FALSE)
        if(flush && !private$locked && any(res)){
          self$swap_out()
          return(TRUE)
        }
        return(FALSE)
      }
      if(self$initialized){
        if(private$new_file){
          self$swap_out()
        }
        nparts = private$dim[private$partition]
        if(partition < 1 || nparts > nparts){
          stop('Partition out of index')
        }
        if(!private$locked){
          # unlink
          unlink(file.path(private$path, sprintf('partition_%d', partition)), force = force)
          unlink(file.path(private$alt_path, sprintf('partition_%d', partition)), force = force)
          if(is.na(replace)){
            return(TRUE)
          }
        }

        # need to write new array
        roi = self$get_roi()
        roi[[private$partition]] = partition
        private$n = private$n + 1
        private$data_env[[private$n]] = list(
          data = array(replace, vapply(roi, length, 0)),
          roi = roi
        )

        if(flush){
          self$swap_out()
        }

        return(TRUE)
      }

      return(FALSE)
    },
    remove = function(force = FALSE){
      if(!force && private$locked){
        stop('Data is locked. Use $remove(force=TRUE) to clear data')
      }
      unlink(private$path, recursive = TRUE, force = TRUE)
      unlink(private$alt_path, recursive = TRUE, force = TRUE)
      private$data_env = list()
      private$n = 0
      private$dim = NULL
      private$dimnames = NULL
      private$attributes = list()
      private$length = 0
      private$locked = FALSE
      private$cached = NULL
      private$index = private$partition = 0
      private$new_file = TRUE
    },
    get_dim = function(){
      private$dim
    },
    set_dim = function(dim){
      # internally used only!!!
      if(length(private$dim) == 0){
        private$dim = dim
      }else{
        stop('set_dim should be used internally')
      }
    },
    get_dimnames = function(){
      private$dimnames
    },
    set_dimnames = function(dimnames){
      # check dim and dimnames
      if(!is.list(dimnames)){
        private$dimnames = NULL
        return(invisible())
      }
      if(!all(vapply(dimnames, is.vector, FUN.VALUE = FALSE))){
        stop('each elements of dimnames should be a vector')
      }
      nms = names(dimnames)
      if(length(dimnames) != length(dim) || any(dim != vapply(dimnames, length, 0))){
        stop('dim does not match with dimnames')
      }
      private$dimnames = dimnames
    },
    set_partition_index = function(which.idx){
      if(private$new_file){
        if(length(private$dim) > 2){
          self$use_partition = TRUE
          private$partition = which.idx
        }
      }else if(private$partition != which.idx){
        stop('Cannot change current partition index from ', private$partition,  ' to ', which.idx)
      }
    },
    get_partition_index = function(){
      private$partition
    },
    stack_print = function(){
      print(str(private$data_env))
      invisible()
    },

    initialize = function(path, read_only = FALSE, format = getOption('hybrid_array.format', default = 'fst')){
      # check path
      if(file.exists(path)){
        info = file.info(path)
        if(!info$isdir){
          stop('path must be a directory')
        }
      }
      # dir.create(path = path, showWarnings = showWarnings, recursive = recursive, mode = mode)
      private$path = path
      private$alt_path = tempfile('junk')
      private$attributes = list()
      private$meta_path = file.path(private$path, '.meta.yaml')
      private$data_env = list()

      # TODO: read meta and set attributes
      if(file.exists(private$meta_path)){
        dat = yaml::read_yaml(private$meta_path, fileEncoding = 'UTF-8')
        for(nm in names(dat)){
          private[[nm]] = dat[[nm]]
        }
        private$new_file = FALSE
        if(read_only){
          self$lock_data()
        }
      }
      if(!length(private$file_format)){
        if(format %in% c('fst', 'csv')){
          private$file_format = format
        }else{
          private$file_format = 'fst'
        }
      }
      .global$arrays[[length(.global$arrays) + 1]] = self
    },

    lock_data = function(){
      private$locked = TRUE
    },
    init_data = function(x, dim, dimnames = NULL, ..., partial = FALSE){
      if(!private$new_file || private$n > 0){
        stop('use add_data')
      }
      if(length(x) == 0){
        return(invisible())
      }
      if(missing(dim)){
        dim = base::dim(x)
      }
      not_match = FALSE

      private$n = private$n + 1
      private$attributes = attributes(x) # might need better way
      private$dimnames = dimnames
      private$length = length(x)
      private$dim = dim
      print(3)
      if(length(private$dim) == 0){
        private$dim = c(private$length, 1)
      }else{
        private$length = prod(private$dim)
      }
      if(
        length(private$dim) != length(dim(x)) ||
        any(private$dim != dim(x))
      ){
        not_match = TRUE
      }
      if(partial){
        all_roi = self$get_roi(...)

        private$data_env[[private$n]] = list(
          data = array(x, sapply(all_roi, length)),
          roi = all_roi
        )

        self$swap_out()
      }else{
        if(not_match){
          private$data_env[[private$n]] = list(
            data = array(x, private$dim),
            roi = lapply(private$dim, seq_len)
          )
        }else{
          private$data_env[[private$n]] = list(
            data = x,
            roi = lapply(private$dim, seq_len)
          )
        }


        self$check_swap()

      }
    },

    check_swap = function(){
      if(private$n == 0){
        return(invisible())
      }
      if(
        .global$array_size() > .global$ram_limit ||
        self$ram_used > self$ram_limit
      ){
        self$swap_out()
      }
    },
    get_roi = function(..., ..duplicate = FALSE){
      roi = list()
      for(ii in seq_along(private$dim)){
        idx = tryCatch({
          ...elt(ii)
        }, error = function(e){
          NULL
        })
        if(is.null(idx)){
          idx = seq_len(private$dim[[ii]])
        }else{
          if(is.logical(idx)){
            if(length(idx) != private$dim[[ii]]){
              stop(sprintf('Dimension %d not match', ii))
            }
            idx = which(idx)
          }else{
            if(!is.integer(idx)){
              idx = as.integer(idx)
            }
            if(!..duplicate && any(duplicated(idx))){
              stop('Duplicated index in dimension ', ii)
            }
          }
        }
        roi[[ii]] = idx

      }
      roi
    },
    alter_data = function(value, ...){
      if(length(value) == 0){
        return(invisible())
      }
      if(private$new_file && private$n == 0){
        warning('initializing data, calling init_data(value)')
        self$init_data(value)
        return(invisible())
      }
      if(...length() != length(private$dim)){
        stop('Dimension not match')
      }
      # generate roi
      roi = self$get_roi(...)
      sub_dim = sapply(roi, length)
      print(sub_dim)
      if(is.atomic(value)){
        value = array(value, sub_dim)
      }else if(length(value) != prod(sub_dim)){
        value = array(value, sub_dim)
      }else if(length(dim(value)) != length(sub_dim)){
        dim(value) = sub_dim
      }

      private$n = private$n + 1
      private$data_env[[private$n]] = list(
        data = value,
        roi = roi
      )

      self$check_swap()
    },

    swap_out = function(){
      use_partition = self$use_partition
      if(private$locked){
        dir.create(private$alt_path, showWarnings = FALSE, recursive = TRUE)
      }
      if(private$n == 0){
        return(invisible())
      }
      dim = private$dim
      ndim = length(dim); ndim = max(ndim, 1)
      # usually use the last for partition and use the index as columns
      # use index when tensor mode >= 2 and use partition when mode >= 3
      if(ndim < 3){
        use_partition = FALSE
        private$partition = 0
      }
      # decide which dimension to be used as partition index
      if(isTRUE(use_partition) && private$partition == 0){
        private$partition = ndim
        if(private$index == ndim){
          private$partition = ndim - 1
        }
      }
      use_partition = private$partition
      private$index = ndim - (use_partition == ndim)
      use_index = private$index
      if(!dir.exists(private$path)){
        dir.create(private$path, showWarnings = FALSE, recursive = TRUE)
      }
      # two cases


      # case 1 partition and index
      tmp_env = new.env(parent = emptyenv())
      lapply(seq_len(private$n), function(idx){
        roi = private$data_env[[idx]]$roi
        tmp_env$idx = 1

        if(use_partition){
          sub_dim = private$dim[-use_partition]
          sub_len = prod(sub_dim)
          w_dim = c(sub_len / private$dim[use_index], private$dim[use_index])
          apply(private$data_env[[idx]]$data, use_partition, function(subx){
            ii = tmp_env$idx
            tmp_env$idx = ii + 1

            part_idx = roi[[use_partition]][ii]

            fname = file.path(private$path, sprintf('partition_%d', part_idx))
            fname_alt = file.path(private$alt_path, sprintf('partition_%d', part_idx))
            if(private$locked && file.exists(fname_alt)){
              old_data = as.matrix(load_table(fname_alt))
              dim(old_data) = sub_dim
            }else if(file.exists(fname)){
              old_data = as.matrix(load_table(fname))
              dim(old_data) = sub_dim
            }else{
              old_data = array(NA, sub_dim)
            }

            old_data = do.call(`[<-`, c(
              list(
                quote(old_data)
              ),
              roi[-use_partition],
              list(
                value = quote(subx)
              )
            ))
            dim(old_data) = w_dim

            old_data = as.data.frame(old_data, row.names = NULL)
            names(old_data) = paste0('V', seq_len(private$dim[use_index]))

            if(private$locked){
              private$write_table(old_data, fname_alt)
            }else{
              private$write_table(old_data, fname)
            }
            NULL
          })
        }else{
          sub_dim = private$dim
          sub_len = prod(sub_dim)
          w_dim = c(sub_len / private$dim[use_index], private$dim[use_index])
          fname = file.path(private$path, sprintf('partition_%d', 1))
          fname_alt = file.path(private$alt_path, sprintf('partition_%d', 1))
          if(private$locked && file.exists(fname_alt)){
            old_data = as.matrix(load_table(fname_alt))
            dim(old_data) = sub_dim
          }else if(file.exists(fname)){
            old_data = as.matrix(load_table(fname))
            dim(old_data) = sub_dim
          }else{
            old_data = array(NA, sub_dim)
          }
          old_data = do.call(`[<-`, c(
            list(
              quote(old_data)
            ), roi,
            list(
              value = quote(private$data)
            )
          ))
          dim(old_data) = w_dim
          if(private$locked){
            private$write_table(old_data, fname_alt)
          }else{
            private$write_table(old_data, fname)
          }
        }
      })


      private$data_env = list()
      private$new_file = FALSE
      private$n = 0

      self$save_meta()


    },

    save_meta = function(){
      if(!private$locked){
        metas = c('attributes', 'partition', 'index', 'dim', 'dimnames', 'locked')
        yaml::write_yaml(
          sapply(metas, function(m){
            private[[m]]
          }, simplify = FALSE, USE.NAMES = TRUE),
          file = private$meta_path,
          fileEncoding = 'UTF-8'
        )
      }

      invisible()
    },

    get_data = function(...){
      if(private$new_file && private$n == 0){
        return(NULL)
      }
      # get index
      # roi = self$get_roi(1,3,4,5)
      roi = self$get_roi(...)
      sub_dim = sapply(roi, length)


      if(private$new_file){
        ii = 2
        dat = private$data_env[[1]]$data
        dat = do.call(`[`, c(
          list(quote(dat)),
          roi,
          list(drop = FALSE)
        ))
      }else{
        # load from files
        columns = roi[[private$index]]
        if(private$partition){
          local_dim = c(private$dim[-c(private$index, private$partition)], sub_dim[private$index])
          dat = sapply(roi[[private$partition]], function(part_idx){
            fname = file.path(private$path, sprintf('partition_%d', part_idx))
            fname_alt = file.path(private$alt_path, sprintf('partition_%d', part_idx))

            local_roi = roi[-c(private$index, private$partition)]
            local_roi[[length(local_roi) + 1]] = seq_along(roi[[private$index]])

            if(private$locked && file.exists(fname_alt)){
              v = as.matrix(private$load_table(fname_alt, columns))
              dim(v) = local_dim

              do.call(`[`, c(
                list(quote(v)),
                local_roi,
                list(drop=FALSE)
              ))
            }else if(file.exists(fname)){
              v = as.matrix(private$load_table(fname, columns))
              dim(v) = local_dim

              do.call(`[`, c(
                list(quote(v)),
                local_roi,
                list(drop=FALSE)
              ))
            }else{
              array(NA, sapply(local_roi, length))
            }


          })
          dim(dat) = c(sub_dim[-c(private$index, private$partition)],
                       sub_dim[private$index], sub_dim[private$partition])

          if(private$partition != length(private$dim)){
            dim_order = c(seq_along(private$dim)[-c(private$index, private$partition)],
                          c(private$index, private$partition))
            dat = aperm(dat, order(dim_order))
          }

        }else{
          fname = file.path(private$path, sprintf('partition_%d', 1))
          fname_alt = file.path(private$alt_path, sprintf('partition_%d', 1))

          columns = roi[[private$index]]
          local_dim = c(private$dim[-c(private$index)], sub_dim[private$index])
          local_roi = roi[-private$index]
          local_roi[[length(local_roi) + 1]] = seq_along(roi[[private$index]])

          if(private$locked && file.exists(fname_alt)){
            v = as.matrix(private$load_table(fname_alt, columns))
            dim(v) = local_dim

            dat = do.call(`[`, c(
              list(quote(v)),
              local_roi,
              list(drop=FALSE)
            ))
          }else if(file.exists(fname)){
            v = as.matrix(private$load_table(fname, columns))
            dim(v) = local_dim

            dat = do.call(`[`, c(
              list(quote(v)),
              local_roi,
              list(drop=FALSE)
            ))
          }else{
            dat = array(NA, sapply(local_roi, length))
          }
        }
        ii = 1
      }

      private$cached = dat

      if(private$n >= ii){
        lapply(seq(ii, private$n), function(n_idx){
          sub_data = private$data_env[[n_idx]]$data
          sub_roi = private$data_env[[n_idx]]$roi
          subsel_dat = lapply(seq_along(private$dim), function(idx){
            r = sapply(sub_roi[[idx]], function(.x){
              r = roi[[idx]] == .x
              if(any(r)){
                which(r)
              }else{
                NA
              }
            })
          })
          subsel_sub = lapply(subsel_dat, function(x){!is.na(x)})
          subsel_dat = lapply(subsel_dat, function(x){x[!is.na(x)]})

          if(all(sapply(subsel_dat, length) > 0)){
            if(any(!unlist(subsel_sub))){
              sub_data = do.call(`[`, c(
                list(quote(sub_data)),
                subsel_sub,
                list(drop=FALSE)
              ))
            }


            private$cached = do.call(`[<-`, c(
              list(quote(private$cached)),
              subsel_dat,
              list(value=quote(sub_data))
            ))
          }
        })
      }


      if(private$new_file){

      }
      dat = private$cached
      private$cached = NULL
      dat

    },

    set_attr = function(key, val){
      if(key %in% c('length', 'dim')){
        stop('cannot set locked attribute: ', key)
      }
      private$attributes[[key]] = val
      self$save_meta()
    },
    get_attr = function(key, default = NULL){
      if(key %in% names(private$attributes)){
        private$attributes[[key]]
      }else{
        default
      }
    }

  ),

  active = list(
    is_hybridarray = function(){ TRUE },
    is_locked = function(){ private$locked },
    ram_used = function(){ lobstr::obj_size(self) },
    initialized = function(){ !(private$new_file && (private$n == 0)) },
    ndims = function(){ length(private$dim) },
    saved_partitions = function(){
      if(private$partition > 0){
        part = private$dim[private$partition]
      }else{
        part = private$dim[length(private$dim)]
      }
      # check file existence
      if(part == 0){
        return(0)
      }

      fname = file.path(private$path, sprintf('partition_%d', seq_len(part)))
      sum(file.exists(fname))
    },
    file_location = function(){
      private$path
    },
    is_namedarray = function(){
      if(is.list(private$dimnames)){
        nms = names(private$dimnames)
        if(length(nms) == length(private$dim) && !'' %in% nms){
          return(TRUE)
        }
      }
      return(FALSE)
    }
  )
)

is_hybridarray <- function(x){
  'R6' %in% class(x) && isTRUE(x$is_hybridarray)
}


