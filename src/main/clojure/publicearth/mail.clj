(ns publicearth.mail)

(defn send-message [& m]
  (let [mail (apply hash-map m)
        props (java.util.Properties.)]

    (doto props
      (.put "mail.smtp.host" (:host mail))
      (.put "mail.smtp.port" (:port mail))
      (.put "mail.smtp.user" (:user mail))
      (.put "mail.smtp.socketFactory.port"  (:port mail))
      (.put "mail.smtp.auth" "true"))

    (if (= (:ssl mail) true)
      (doto props
        (.put "mail.smtp.starttls.enable" "true")
        (.put "mail.smtp.socketFactory.class"
              "javax.net.ssl.SSLSocketFactory")
        (.put "mail.smtp.socketFactory.fallback" "false")))

    (let [authenticator (proxy [javax.mail.Authenticator] []
                          (getPasswordAuthentication
                           []
                           (javax.mail.PasswordAuthentication.
                            (:user mail) (:password mail))))
          session (javax.mail.Session/getDefaultInstance props authenticator)
          msg     (javax.mail.internet.MimeMessage. session)]

      (.setFrom msg (javax.mail.internet.InternetAddress. (:user mail)))
      (doseq [to (:to mail)]
        (.setRecipients msg
                        (javax.mail.Message$RecipientType/TO)
                        (javax.mail.internet.InternetAddress/parse to)))
      (.setSubject msg (:subject mail))
      (.setText msg (:text mail))
      (javax.mail.Transport/send msg))))

