package com._4paradigm.flowengine.pipeline.core.functions.java.groovy.prerank
def order() {
    "ASC"
}
def score(item) {
    return item
}
def size() {
    -1
}
def execute(context, args) {
    def l=args[0]
    l=l.sort {a,b-> if(order() == "ASC") {score(a)<=>score(b) } else {score(b)<=>score(a)}} as List
    def size = size()
    if(size >= 0) {
        l=l.subList(0, Math.min(l.size(), size))
    }
    return l
}

def str = [1, 3, 2]

println execute(null, [str,"DESC"])

