Corresponding code is in the folder `semantics`.

## EVM without opcodes (Ether transfers only)
We start with looking at a very restricted version of EVM, having no opcodes. That means only Ether transfers are possible.
Even that seemingly simple case already has a relatively complex semantics.

First, we would like to define what kind semantics we are looking for. Ethereum is a state machine, which has a global state, and
transactions that trigger some state transition. There are also some state transitions that happen at the end of each block. These are
related to miner and ommer rewards. The transition from one state to another is deterministic. Given the initial state, some extra
environment (recent header hashes, current block number, timestamp of the current block), and the transaction object, there is only
one possible valid end state of the transition. For this reason, the state transitions are often described as a state
transition function:

`STATE_end = STATE_TR_FUNC(STATE_init, env, tx)`

But this is not the only way to describe state transitions.
And it is not always the most convenient way. In our approach, we will instead describe state transitions as a set of boolean expressions.
They are boolean in the sense that they can be evaluated to either `true` or `false`, but they can rely on variety of types.
These expressions will only tell us whether certain combinations of the initial state, environment, transaction object, and the end state,
are all part of a valid state transition. However, they do not have a goal of helping us to determine (compute) the end state from
the initial state, environment, and the transaction object. So they look more like this:

`EXPRESSION(STATE_init, env, tx, STATE_end) == true?`

Moreover, this representation allows for some non determinsm, which means that there could be some extra "oracle" input that helps the
evaluation:

`EXPRESSION(STATE_init, env, tx, STATE_end, ORACLE_input) == true?`

From our experience with EVM and Ethereum, we know that each transaction can normally be decomposed into series of intermediate transitions.
These intermediate transitions could be various internal accounting steps of Ethereum (like purchasing gas for Ether, or gas refunds), or
steps of the EVM interpreter. In other words, the decomposition could look something like this:

`EXPRESSION_1(STATE_init, env, tx, STATE_1, ORACLE_input1) == E1`

`EXPRESSION_2(STATE_1, env, tx, STATE_2, ORACLE_input2) == E2`

`EXPRESSION_3(STATE_2, env, tx, STATE_3, ORACLE_input3) == E3`

`...`

`EXPRESSION_N(STATE_N-1, env, tx, STATE_end, ORACLE_inputN) == EN`

How do these expressions re-compose back into the `EXPRESSION`?. First of all, we need to add another condition:

`E1 && E2 && E3 && ... && EN == true?`

Obviously, oracle input of the `EXPRESSION` could simply be a collection of all individual oracle inputs.

Next observation we make is that the intermediate transitions are usually quite "local", which means that if we compare
the initial state and the end state of each of those transitions, the difference between can be described succinctly. This
can be understood as the length of the description having a pre-determined upper bound. Since there is no upper bound on
the size of the state, we need a way of specifying part of the state the state transition modifies. At this point, we
choose to represent the state as an array of accounts. Here, we use "array" in the context of theory of arrays. In this
theory, there are two main functions defined for an array: `select` and `store`. Expression `select(a, i)` evaluates to the
`i`-th element of the array `a`. Expression `store(a, i, v)` evaluates to a new array which is equal to the array `a`
except for the value in the index `i`, where `select(store(a, i, v), i) == v`. If the state is an array of accounts,
what is an account? We define it as a structure consisting of 5 fields: balance, nonce, code length, code, and storage.
Balance, nonce, and code length are integers. Code is an array of integers (one integer per byte of bytecode).
Storage is also an array of integers. Since the length of the storage is not observable from within the EVM, we do not need
to represent it.

Transaction is a structure. The field which we will be concerned with first is the sender. Although we are used to think of
sender as being an address, which is 20-byte string, for the purpose of semantic description, it will be an integer,
usually quite large, in the range `[0..2^160)`. These integers are used as indices in the state array. Therefore,
if `tx.sender` is the variable used for transaction sender, and `state` is the variable used for the state,
sender's balance is `select(state, tx.sender).balance`, and nonce is `select(state, tx.sender).nonce`. In Ethereum,
a transaction is considered to be valid to be applied to a state (and hence included into a block), if two conditions
are satisfied:
1. `select(state, tx.sender).nonce = tx.nonce`
2. `select(state, tx.sender).balance >= tx.gasLimit * tx.gasPrice`

If these two conditions are satisfied, and we assume that the transaction made it into a block, the sender must
"purchase" the gas. It is important to do it here, because once gas is purchased, it can only be given back after
the transaction's full execution, either in the case of no error, or by running into a `REVERT` opcode in the top-most
execution context. Therefore, at this point, the state needs to be modified to subtract the cost of the
purchased gas. It is important, because during the execution, the balance of the sender's account can be observed by
a `BALANCE` opcode, and situations where subtraction was made and not made can be distinguished, and lead to
different outcomes. In order to describe such state transition, we will use the notation of substitution rules
(these can always be transformed into logical expressions). Our rules will consist of three parts: pattern,
substitution, and guard.

In the pattern, we describe the initial state of the transition. Pattern matching is a powerful resource, and it
comes with these aspects:
1. It allows matching a part of a sequence while ignoring the rest of that sequence.
2. It can match types, letting us avoid the type-reflection functions in the semantics.
3. It can bind variables, to be used in the other two parts of the rule - substitution and guard.
We hope that the destructuring aspect of pattern matching will not be required for our purposes.

In the substitution, we describe how the part matched by the pattern in the initial state will look like
in the end state. Substitution can use any variables bound by the pattern.

The guard is a boolean expression. It contains at least one variable bound by the pattern, and specifies the
condition under which the entire rule is applicable. If the guard evaluates to `false`, and, consequently,
the rule is not applicable, there can be another rule that is applicable. Since EVM is deterministic by design,
we expect that in a correct semantics, at most one rule is applicable in any given configuration.

Our gas purchasing transition can be expressed by the following rule:

```
state, tx ==> store(state, tx.sender, account{
                                          balance: prevbalance - cost,
                                          nonce: prevaccount.nonce,
                                          codelength: prevaccount.codelength,
                                          code: prevaccount.code,
                                          storage: prevaccount.storage
                                       }), tx.gasLimit, tx
WHERE prevaccount = select(state, tx.sender)
      prevbalance = prevaccount.balance
      cost = tx.gasLimit * tx.gasPrice
WHEN prevaccount.nonce = tx.nonce AND prevbalance >= cost
```

We have introduced `WHERE` section to define some variable for convenience, and placed the guard into the `WHEN` section.
Pattern and substitution are on the first line, separated by the arrow `==>`. One needs to notice that the substitution
contains three-element sequence whereas the pattern contains only two elements. If the gas purchase is successful,
the "gas" object gets introduced. It will be useful for the subsequent rules, because now we can make all of them
simply require the presence of that "gas" object, instead of repeating the guards of the gas purchasing rule.
