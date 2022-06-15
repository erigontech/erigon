import random
import sys

class Node:
    def __init__(self, k):
        self.key = k
        self.left = None
        self.right = None

def bst_insert(t, x):
    if t == None:
        return Node(x)
    root = t
    while True:
        if t.key >= x:
            if t.left == None:
                t.left = Node(x)
                return root
            t = t.left
        else:
            if t.right == None:
                t.right = Node(x)
                return root
            t = t.right
    
def bst_search(t, x):
    while t != None:
        if x < t.key:
            t = t.left
        elif x > t.key:
            t = t.right
        else:
            return True
    return False

def print_in_order(t):
    if t != None:
        print_in_order(t.left)
        print(t.key)
        print_in_order(t.right)

def print_in_reverse_order(t):
    if t != None:
        print_in_reverse_order(t.right)
        print(t.key)
        print_in_reverse_order(t.left)

def tree_min(t):
    if t != None:
        while t.left != None:
            t = t.left
        return t.key

def tree_max(t):
    if t != None:
        while t.right != None:
            t = t.right
        return t.key

def random_tree(n):
    t = None
    for i in range(n):
        t = bst_insert(t,random.randint(0,2*n))
    return t

def right_rotate(t):
    assert t != None
    assert t.left != None
    i = t.left
    t.left = i.right
    i.right = t
    return i

class Canvas:
    def __init__(self,width):
        self.line_width = width
        self.canvas = []

    def put_char(self,x,y,c):
        if x < self.line_width:
            pos = y*self.line_width + x
            l = len(self.canvas)
            if pos < l:
                self.canvas[pos] = c
            else:
                self.canvas[l:] = [' ']*(pos - l)
                self.canvas.append(c)

    def print_out(self):
        i = 0
        for c in self.canvas:
            sys.stdout.write(c)
            i = i + 1
            if i % self.line_width == 0:
                sys.stdout.write('\n')
        if i % self.line_width != 0:
            sys.stdout.write('\n')

def print_binary_tree_r(t,x,y,canvas):
    max_y = y
    if t.left != None:
        x, max_y, lx, rx = print_binary_tree_r(t.left,x,y+2,canvas)
        x = x + 1
        for i in range(rx,x):
            canvas.put_char(i, y+1, '/')

    middle_l = x
    for c in str(t.key):
        canvas.put_char(x, y, c)
        x = x + 1
    middle_r = x

    if t.right != None:
        canvas.put_char(x, y+1, '\\')
        x = x + 1
        x0, max_y2, lx, rx = print_binary_tree_r(t.right,x,y+2,canvas)
        if max_y2 > max_y:
            max_y = max_y2
        for i in range(x,lx):
            canvas.put_char(i, y+1, '\\')
        x = x0

    return (x,max_y,middle_l,middle_r)

def print_tree(t):
    print_tree_w(t,80)

def print_tree_w(t,width):
    canvas = Canvas(width)
    print_binary_tree_r(t,0,0,canvas)
    canvas.print_out()

def findDepthAndConstruct(root, n, d):
    if n >= len(d):
        d.append(1)
    else:
        d[n] += 1
    if root.left == None and root.right == None:
        return n
    if root.left == None:
        return findDepthAndConstruct(root.right, n+1, d)
    if root.right == None:
        return findDepthAndConstruct(root.left, n+1, d)
    return max(findDepthAndConstruct(root.left, n+1, d), findDepthAndConstruct(root.right, n+1, d))

def isPerfectlyBalance(root):
    d = []
    findDepthAndConstruct(root, 0, d)
    for i in range(len(d)):
        if 2 ** i != d[i] and i != len(d) - 1:
            return False
    return True
    
t = random_tree(4)
print_tree(t)
print(isPerfectlyBalance(t))