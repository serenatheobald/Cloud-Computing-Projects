For Homework 3, my files are: ds561hw3pythonhelper.py (helper python client), main.py (my main code), and requirements.txt (listing my dependencies).

In addition, I made very small changes to the helper client .py file. With the original helper python client file, I wasn't able to directly access files while requesting even just testing the functionality of a few hundred of my cloud storage files. I recieved only "404 Error Not Found" messages for each html file I was trying to access, which made me think that the server was unable to locate the specified file.

Here is the new code I implemented:



**URL Parameter for Requests:**
Old: conn.request("GET", filename, headers=headers)
New: url = f"/accept_requests?file_name={filename}"; conn.request("GET", url, headers=headers)
In the new code, instead of directly requesting the filename as a path, it requests an endpoint /accept_requests with a parameter file_name holding the desired filename.

**Filename Creation:**
Old: filename = make_filename(args.bucket, args.webdir, args.index)
New: filename = make_filename(args.bucket, args.webdir, args.index).split('/')[-1]

The new code I implemented only takes the last part of the generated filename, effectively removing any directory predixes

**Domain Modifications:**
Added this line of code: args.domain = args.domain.replace("http://", "").replace("https://", "")
In the newer version, any 'http://' or 'https://' prefix from the domain argument is removed. This step ensures that only the domain name is passed to the make_request function.
