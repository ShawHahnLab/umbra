#!/usr/bin/env python

from mailer import Mailer

m = Mailer()
m.mail(to_addrs = 'jesse08@gmail.com',
       subject = "Hi There",
       msg_body = "A few words\nin a plaintext message",
       msg_html = "<b><i>Ooooh HTML</i></b>")
