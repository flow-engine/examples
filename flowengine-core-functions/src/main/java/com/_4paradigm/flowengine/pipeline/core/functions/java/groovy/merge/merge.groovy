package com._4paradigm.flowengine.pipeline.core.functions.java.groovy.merge
def order() {
    "DESC"
}
def basescore(item) {
    item['score']
}
def weights() {
    [0.3,0.7]
}
def mergeItems(items) {
    items.inject([:], {cache, item-> if(cache) cache['merge_score']+=item['merge_score'] else cache = item;return cache})
}
def merge(lists) {
    def l = lists.inject([], {cache,list->cache+list})
    def groupedlist = l.groupBy {item->item['itemId']}
    return groupedlist.inject([], {cache, items->cache+= mergeItems(items.value)})
}
def sort(list) {
    list.sort {a,b-> if(order() == "ASC") {a['merge_score']-b['merge_score'] } else {b['merge_score']-a['merge_score']}}
}
def weightedscore(lists,weights) {
    return lists.eachWithIndex {list,index-> list.each{item->item['merge_score']=basescore(item)*weights[index]}}
}
def cut(list) {
    def s=size()
    if(s>=0) {
        list=list.subList(0, Math.min(list.size(), s))
    }
    return list
}
def execute(context, args) {
    return cut(sort(merge(weightedscore(args, weights()))))
}
def l1=[['itemId':'0','score':11],['itemId':"1",'score':11], ['itemId':'2','score':1]]
def l2=[['itemId':'1','score':11],['itemId':'2', 'score':0]]
//print sort(merge(weightedscore([l1,l2], weights())))

print execute(null, [l1,l2])

