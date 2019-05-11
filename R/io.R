# IO methods
load_table_fst <- function(file, columns = NULL){
  if(is.null(columns)){
    re = fst::read_fst(path = file)
  }else{
    if(is.logical(columns)){
      columns = which(columns)
    }
    if(!is.integer(columns)){
      warning('columns will be converted to integers')
      columns = as.integer(columns)
    }
    if(any(duplicated(columns))){
      stop('duplicated columns')
    }
    columns = paste0('V', columns)
    re = fst::read_fst(path = file, columns = columns)
  }

}
write_table_fst <- function(x, file, compress = 0, ...){
  fst::write_fst(x, file, compress = compress, ...)
}

load_table_dt <- function(file, columns = NULL){
  re = data.table::fread(file)
  if(!is.null(columns)){
    re = re[, columns]
  }
  re
}
write_table_dt <- function(x, file){
  data.table::fwrite(x, file = file, row.names = FALSE)
}
