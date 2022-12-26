(ns org.rssys.encrypt-test
  (:require
    [clojure.test :as test :refer [deftest testing is]]
    [matcho.core :as m :refer [match]]
    [org.rssys.encrypt :as sut])
  (:import
    (javax.crypto
      Cipher)))


(def plain-text "Suppose the original message has length = 50 bytes")


(deftest new-iv-test
  (testing "generate IV for AES/GCM mode"
    (let [iv (sut/new-iv)]

      (testing "should produce output 12 bytes length"
        (m/assert sut/iv-length (alength iv))
        (m/assert 12 sut/iv-length)))))


(deftest gen-secret-key-test

  (testing "generate secret key from string token"
    (let [secret-token "qMkaS3^%&%@lOIOJN7h7sbrgojv"
          result       (sut/gen-secret-key secret-token)]

      (testing "should produce a byte array"
        (is (bytes? result)))

      (testing "should produce output 256-bit length"
        (m/assert 256 (* 8 (alength result))))))

  (testing "generate secret keys from the same token"
    (let [secret-token "qMkaS3^%&%@lOIOJN7h7sqbrgojv"
          secret-bytes1 (sut/gen-secret-key secret-token)
          secret-bytes2 (sut/gen-secret-key secret-token)]

      (testing "should always produce the same output"
        (m/assert (into [] secret-bytes1) (into [] secret-bytes2)))))

  (testing "generate secret keys from different tokens"
    (let [password1     "1234567890"
          password2     "123456789"
          secret-bytes1 (sut/gen-secret-key password1)
          secret-bytes2 (sut/gen-secret-key password2)]

      (testing "should always produce different output"
        (m/dessert (into [] secret-bytes1) (into [] secret-bytes2))))))


(deftest init-cipher-test

  (testing "init cipher"
    (testing "in encryption mode"
      (let [cipher (sut/init-cipher (sut/gen-secret-key "123456") :encrypt (.getBytes "1234567890ab"))]
        (testing "should produce Cipher instance"
          (is (instance? Cipher cipher)))))

    (testing "in decryption mode"
      (let [cipher (sut/init-cipher (sut/gen-secret-key "123456") :decrypt (.getBytes "1234567890ab"))]
        (testing "should produce Cipher instance"
          (is (instance? Cipher cipher)))))

    (testing "should catch wrong cipher mode"
      (is (thrown-with-msg? Exception #"Wrong cipher mode"
            (sut/init-cipher (sut/gen-secret-key "123456") :bad-mode (.getBytes "1234567890ab")))))))



(deftest encrypt-data-test

  (testing "encryption"
    (let [result (sut/encrypt-data (sut/gen-secret-key "123456") (.getBytes plain-text))]

      (testing "should produce a byte array"
        (m/assert bytes? result))

      (testing "should produce output with size more than plain text size"
        (is (> (count result) (.length plain-text)))))))


(deftest decrypt-test

  (testing "decryption"
    (let [secret-key (sut/gen-secret-key "123456")
          encrypted-bytes (sut/encrypt-data secret-key (.getBytes plain-text))
          corrupted-bytes (byte-array (update (into [] encrypted-bytes) 42 inc))
          result (String. (sut/decrypt-data secret-key encrypted-bytes))]

      (testing "should produce decrypted text equal to original text"
        (m/assert plain-text result))

      (testing "should detect corrupted data for decryption"
        (is (thrown-with-msg? Exception #"Tag mismatch"
              (sut/decrypt-data secret-key corrupted-bytes)))))))

