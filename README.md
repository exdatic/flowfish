![build](https://github.com/exdatic/flowfish/actions/workflows/python-package.yml/badge.svg)

# flowfish ><(((('>

Yet another pythonic workflow engine.

## About

The lightweight open source library flowfish is a great way to manage your ML lifecycle without all the hassle of third party dependencies. With its simple json file, flowfish makes it easy to connect asynchronous and synchronous Python functions while keeping your code free from any unwanted dependencies.

Intermediate results are automatically saved to disk so you can pick up where you left off. Flowfish's dependency graph keeps track of parameter changes and only reruns a function when its parameter has changed. This makes it easy to work with large data sets and iterate quickly on your analyses.

Flowfish automatically wraps any long-running iterable with tqdm. This makes your loops show a smart progress meter that is updated in real-time. This is extremely helpful when trying to optimize code or track the progress of a process.

If you need to run a Python function from your local Jupyter notebook directly on Google Colab, flowfish automatically uploads all the code and data required for the function. This is handy if you want to use powerful GPU resources that can perform heavy computations without having to manually upload data and install all the dependencies.

## 🐍 Installation

- **Operating system**: macOS / OS X · Linux · Windows
- **Python version**: Python 3.7+
- **Package managers**: [pip]

```bash
pip install flowfish
```

## ✨ Getting started

This getting started tutorial demonstrates how things work. It is not a real world example, we just want to sum up some numbers.

First we define a function `add()` with two arguments `a` and `b`, that are later added together. Then we assign it to a node named `sum` using a JSON config. And finally we set the default values for `a` and `b` to 3 and 4. Remember, it is just an example.

```python
from flowfish import flow

def add(a, b):
    return a + b

f = flow({
    "math": {
        "sum@add": {
            "a": 3,
            "b": 4
        }
    }
})
```

Now we call the ``sum()`` function and the default values for `a` and `b` are applied implicitly.

```python
f.math.sum()
```

👉 7

Now we call `sum()` with `a` and `b` set explicitly.

```python
f.math.sum(5, 6)
```

👉 11

Now we replace our custom `add()` function with Python's built-in `sum()` function, which has a slightly different signature: `sum(iterable, start=0)`.

```python
from flowfish import flow

f = flow({
    "math": {
        "sum": {
            "iterable": [3, 4]
        }
    }
})
```

```python
f.math.sum()
```

👉 7

Now we connect some nodes together and build our first flow. As already mentioned, a node is actually a Python function. So when we connect nodes together, we connect functions together. If we want to connect a node with a value, we can just assign the value to a node parameter or we can use the built-in flow function `map()` that takes the value as  `input` and simply returns it.

```python
from flowfish import flow

f = flow({
    "math": {
        "number_one@map": {
            "input": 3
            
        },
        "number_two@map": {
            "input": 4
        },
        "sum": {
            "iterable": ["@number_one", "@number_two"]
        }
    }
})
```

```python
f.math.sum()
```

👉 7

Now we visualize the flow graph.

```python
-f.math.sum
```

![svg](https://raw.githubusercontent.com/exdatic/flowfish/main/images/output_14_0.svg)

## 📚 Usage

### Scopes and Nodes

The flow is configured in JSON format and consists of _scopes_ and _nodes_. A scope is a group of nodes and a node is just an alias for a pure Python function.

A basic flow configuration looks like this:

```js
{
    "scope": {
        "node": {
        }
    }
}
```

* scope and node names may only contain ASCII letters, digits or underscores
* config keys starting with `#` are considered comments and therefore ignored, this is usefull for temporarily disabling nodes or scopes

### Scope and node inheritance

Scopes and nodes can inherit their properties from other scopes and nodes by useing the `@` notation.

```js
{
    "example": {
        "foo": {
        },
        "bar@foo": {
        }
    }
}
```

A scope can inherit from:

- another scope from the current flow
- another scope from an external config file, e.g. "../foo.json#foo"

A node can inherit from:

- another node from the current scope
- a function from some Python module, e.g. "sklearn.model_selection.train_test_split"
- a function from the `__main__` module
- a built-in Python function, e.g. "open"
- a class (here the constructor is considered as function), e.g. "foo.bar.FooBar"

### Property assignment

Nodes can get their property values from the return values of other nodes.

### Function results

A leading `@` assigns the return value of a node function to a node property.

```js
{
    "example": {
        "foo": {
        },
        "bar": {
            "foo": "@foo"
        }
    }
}
```

### Paths

A `/` after the node name assigns the node path. Additionally another path can be appended.

```js
{
    "example": {
        "foo": {
        },
        "bar": {
            "path": "@foo/test.text"
        }
    }
}
```

### References

A leading `&` assigns a reference to the node function itself as opposed to the result value.

```js
{
    "example": {
        "foo": {
        },
        "bar": {
            "path": "&foo"
        }
    }
}
```

### Quoting

String literals starting with the reserved character `@` or `&` must be quoted by appending the same character again (e.g. `@@` or `&&`).

### Result caching

Node results are cached by default. Nodes with the `_dump` property set are pickled to a `.dump` file and must not be reprocessed again when called later.

### Progress bars

Nodes with the `_tqdm` property set are wrapped with a tqdm progress bar if their functions are valid python generators.

### Property overriding

```js
{
    "example": {
        "_props": {
            "tokenizer.language": "klingon",
            "analyzer.language": "klingon"
        },
        ,
        "tokenizer": {
        },
        "analyzer": {
        }
    }
}
```

## Flow command line tool

```bash
% flow
usage: flow [-h] {run,agent,push,pull,prune} ...

optional arguments:
  -h, --help            show this help message and exit

command:
  {run,agent,push,pull,prune}
    run                 run flow
    agent               start agent
    push                push data to sync_dir
    pull                pull data from sync_dir
    prune               prune files in data_dir
```

## License

See [LICENSE](LICENSE).
