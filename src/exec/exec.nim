type ExecNode* = ref object
  name*: string

proc newExecNode*(name: string = "stub"): ExecNode =
  ExecNode(name: name)
