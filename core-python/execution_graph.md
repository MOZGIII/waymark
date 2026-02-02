taking the example

results = []

for item in items:
    action_result = @action(item)
    results.append(action_result + 1)

we should have

-> for_increment
-> action sets to action[0]
-> results (current set to [ action[0] + 2 ])
-> for increment
-> action sets to action[1]
-> results (current set to [ action[0] + 2, action[1] + 2 ])

all variables that reference a action result should contain the expression of the value instead of the value itself, to allow us to dynamically insert it when we replay the graph
