package ssh

object test {
    class MyClass {
        def aaa(): MyClass = {
            println("aaa")
            this // 返回当前对象
        }

        def bbb(): MyClass = {
            println("bbb")
            this // 返回当前对象
        }

        def ccc(): MyClass = {
            println("ccc")
            this // 返回当前对象
        }
    }

    val obj = new MyClass()
    obj.aaa().bbb().ccc()
    println("--------------")
    obj.ccc().bbb().aaa()

    def main(args: Array[String]): Unit = {

    }
}
