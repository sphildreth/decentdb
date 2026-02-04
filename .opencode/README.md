# Opencoder Skills (DecentDB)

This folder contains reusable prompts (“skills”) to paste into Opencoder when you want consistent behavior on certain task types.

## How to use

### Option A: Copy/paste from file

1. Open the relevant skill in `.opencoder/skills/`.
2. Paste its content at the top of your Opencoder task prompt.
3. Add your specific task below the skill.

### Option B: CLI inject (recommended)

- Print the Nim skill to stdout:
	- `cat .opencoder/skills/nim.md`

- Use shell command substitution to prepend the skill to a prompt you type inline:
	- `opencoder "$(cat .opencoder/skills/nim.md)

<YOUR TASK HERE>"`

- Or build a prompt file that includes the skill (easy to review/diff):
	- `cat .opencoder/skills/nim.md my_task.md > /tmp/opencoder_prompt.md`
	- `opencoder "$(cat /tmp/opencoder_prompt.md)"`

These skills are intentionally conservative: they bias toward correctness, minimal diffs, and test-driven changes.
