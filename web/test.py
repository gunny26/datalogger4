import web             #web.py framework

urls = (
    '/.*', 'hello',    #url pattern for web.py to call hello.GET beneath
    )

class hello:
    def GET(self):
        #some browsers don't like plain text, so I use html
        web.header( 'Content-type', 'text/html' )
        return str("Hello")            # return it as a HTTP response

#sets web.py's func as WSGI entry point
application = web.application(urls, globals()).wsgifunc()

if __name__ == '__main__':
    main()
