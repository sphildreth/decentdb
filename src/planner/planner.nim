type Plan* = ref object
  description*: string

proc newPlan*(desc: string = "stub"): Plan =
  Plan(description: desc)
