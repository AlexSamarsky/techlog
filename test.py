
class Class1:
    def p(self):
        print('p: class1')

    def s(self):
        print('s: class1')
class Class2:
    def p(self):
        print('p: class2')



class Class3(Class2, Class1):

    pass

asdf = Class3()
asdf.p()