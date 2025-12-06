type task = { request : Models.ExecuteQueryRequest.t }
type status = Models.QueryStatus.t

type err =
  { message : string
  ; details : string
  }

type res_ =
  { query_definition : Models.QueryDefinition.t
  ; result_id : Uuid.t option
  }

type result_ = (res_, err) result
