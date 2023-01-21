(ns org.rssys.swim.encrypt
  "Cryptographic functions"
  (:import
    (java.io
      ByteArrayInputStream
      ByteArrayOutputStream)
    (java.security
      SecureRandom)
    (javax.crypto
      Cipher
      SecretKeyFactory)
    (javax.crypto.spec
      GCMParameterSpec
      PBEKeySpec
      SecretKeySpec)))


(def ^:const iv-length 12)


(defn new-iv
  "Create new random init vector using SecureRandom.
  Returns byte array `iv-length` bytes length with random data."
  []
  (let [iv-array (byte-array iv-length)]
    (.nextBytes (SecureRandom.) iv-array)
    iv-array))


(defn gen-secret-key
  "Generate secret key based on a given token string.
  Returns bytes array 256-bit length."
  [^String secret-token]
  (let [salt    (.getBytes "org.rssys.password.salt.string!!")
        factory (SecretKeyFactory/getInstance "PBKDF2WithHmacSHA256")
        spec    (PBEKeySpec. (.toCharArray secret-token) salt 10000 256)]
    (.getEncoded (.generateSecret factory spec))))


(defn init-cipher
  "Init cipher using given secret-key (32 bytes) and IV (`iv-length` bytes).
  Cipher mode may be :encrypt or :decrypt
  Returns ^Cipher."
  ^Cipher
  [^bytes secret-key cipher-mode ^bytes iv-bytes]
  (let [cipher             ^Cipher (Cipher/getInstance "AES/GCM/NoPadding")
        gcm-tag-length-bit 128
        gcm-iv-spec        (GCMParameterSpec. gcm-tag-length-bit iv-bytes)
        cipher-mode-value  (cond
                             (= :encrypt cipher-mode) Cipher/ENCRYPT_MODE
                             (= :decrypt cipher-mode) Cipher/DECRYPT_MODE
                             :else (throw (ex-info "Wrong cipher mode" {:cipher-mode cipher-mode})))]
    (.init cipher ^int cipher-mode-value (SecretKeySpec. secret-key "AES") gcm-iv-spec)
    cipher))


(defn encrypt-data
  "Encrypt plain data using given secret key.
   Returns bytes array: IV (12 bytes) + encrypted data."
  ^bytes
  [^bytes secret-key ^bytes plain-data]
  (let [baos           (ByteArrayOutputStream.)
        cipher         (init-cipher secret-key :encrypt (new-iv))
        encrypted-data (.doFinal cipher plain-data)]
    (.write baos ^bytes (.getIV cipher))
    (.write baos ^bytes encrypted-data)
    (.toByteArray baos)))


(defn decrypt-data
  "Decrypt data using given secret key.
   Returns bytes array: plain data."
  ^bytes
  [^bytes secret-key ^bytes encrypted-data]
  (let [bais   (ByteArrayInputStream. encrypted-data)
        iv     (byte-array iv-length)
        _      (.read bais iv)
        buffer (byte-array (.available bais))
        _      (.read bais buffer)
        cipher (init-cipher secret-key :decrypt iv)]
    (.doFinal cipher buffer)))
