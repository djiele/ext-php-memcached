<?php

namespace Djiele\Memcached\Sasl;

class SaslAuthDigestMd5
{
    /**
    * Provides the (main) client response for DIGEST-MD5
    * requires a few extra parameters than the other
    * mechanisms, which are unavoidable.
    * 
    * @param  string $authcid   Authentication id (username)
    * @param  string $pass      Password
    * @param  string $challenge The digest challenge sent by the server
    * @param  string $hostname  The hostname of the machine you're connecting to
    * @param  string $service   The servicename (eg. imap, pop, acap etc)
    * @param  string $authzid   Authorization id (username to proxy as)
    * @return string            The digest response (NOT base64 encoded)
    */
    function getChallengeResponse($authcid, $pass, $challenge, $hostname, $service, $authzid = '')
    {
        $challenge = $this->parseChallenge($challenge);
        $authzid_string = '';
        if ($authzid != '') {
            $authzid_string = ',authzid="' . $authzid . '"'; 
        }

        if (!empty($challenge)) {
            $cnonce         = $this->computeClientNonce();
            $digest_uri     = sprintf('%s/%s', $service, $hostname);
            $response_value = $this->formatResponse($authcid, $pass, $challenge['realm'], $challenge['nonce'], $cnonce, $digest_uri, $authzid);

            if ($challenge['realm']) {
                return sprintf('username="%s",realm="%s"' . $authzid_string  .
',nonce="%s",cnonce="%s",nc=00000001,qop=auth,digest-uri="%s",response=%s,maxbuf=%d', $authcid, $challenge['realm'], $challenge['nonce'], $cnonce, $digest_uri, $response_value, $challenge['maxbuf']);
            } else {
                return sprintf('username="%s"' . $authzid_string  . ',nonce="%s",cnonce="%s",nc=00000001,qop=auth,digest-uri="%s",response=%s,maxbuf=%d', $authcid, $challenge['nonce'], $cnonce, $digest_uri, $response_value, $challenge['maxbuf']);
            }
        } else {
            return PEAR::raiseError('Invalid digest challenge');
        }
    }
    
    /**
    * Parses and verifies the digest challenge*
    *
    * @param  string $challenge The digest challenge
    * @return array             The parsed challenge as an assoc
    *                           array in the form "directive => value".
    * @access private
    */
    protected function parseChallenge($challenge)
    {
        $tokens = array();
        while (preg_match('/^([a-z-]+)=("[^"]+(?<!\\\)"|[^,]+)/i', $challenge, $matches)) {

            // Ignore these as per rfc2831
            if ($matches[1] == 'opaque' OR $matches[1] == 'domain') {
                $challenge = substr($challenge, strlen($matches[0]) + 1);
                continue;
            }

            // Allowed multiple "realm" and "auth-param"
            if (!empty($tokens[$matches[1]]) AND ($matches[1] == 'realm' OR $matches[1] == 'auth-param')) {
                if (is_array($tokens[$matches[1]])) {
                    $tokens[$matches[1]][] = preg_replace('/^"(.*)"$/', '\\1', $matches[2]);
                } else {
                    $tokens[$matches[1]] = array($tokens[$matches[1]], preg_replace('/^"(.*)"$/', '\\1', $matches[2]));
                }

            // Any other multiple instance = failure
            } elseif (!empty($tokens[$matches[1]])) {
                $tokens = array();
                break;

            } else {
                $tokens[$matches[1]] = preg_replace('/^"(.*)"$/', '\\1', $matches[2]);
            }

            // Remove the just parsed directive from the challenge
            $challenge = substr($challenge, strlen($matches[0]) + 1);
        }

        /**
        * Defaults and required directives
        */
        // Realm
        if (empty($tokens['realm'])) {
            $tokens['realm'] = "";
        }

        // Maxbuf
        if (empty($tokens['maxbuf'])) {
            $tokens['maxbuf'] = 65536;
        }

        // Required: nonce, algorithm
        if (empty($tokens['nonce']) OR empty($tokens['algorithm'])) {
            return array();
        }

        return $tokens;
    }

    /**
    * Creates the response= part of the digest response
    *
    * @param  string $authcid    Authentication id (username)
    * @param  string $pass       Password
    * @param  string $realm      Realm as provided by the server
    * @param  string $nonce      Nonce as provided by the server
    * @param  string $cnonce     Client nonce
    * @param  string $digest_uri The digest-uri= value part of the response
    * @param  string $authzid    Authorization id
    * @return string             The response= part of the digest response
    */    
    protected function formatResponse($authcid, $pass, $realm, $nonce, $cnonce, $digest_uri, $authzid = '')
    {
        if ($authzid == '') {
            $A1 = sprintf('%s:%s:%s', pack('H32', md5(sprintf('%s:%s:%s', $authcid, $realm, $pass))), $nonce, $cnonce);
        } else {
            $A1 = sprintf('%s:%s:%s:%s', pack('H32', md5(sprintf('%s:%s:%s', $authcid, $realm, $pass))), $nonce, $cnonce, $authzid);
        }
        $A2 = 'AUTHENTICATE:' . $digest_uri;
        return md5(sprintf('%s:%s:00000001:%s:auth:%s', md5($A1), $nonce, $cnonce, md5($A2)));
    }

    /**
    * Creates the client nonce for the response
    *
    * @return string  The cnonce value
    */
    protected function computeClientNonce()
    {
        if (@file_exists('/dev/urandom') && $fd = @fopen('/dev/urandom', 'r')) {
            return base64_encode(fread($fd, 32));

        } elseif (@file_exists('/dev/random') && $fd = @fopen('/dev/random', 'r')) {
            return base64_encode(fread($fd, 32));

        } else {
            $str = '';
            for ($i=0; $i<32; $i++) {
                $str .= chr(mt_rand(0, 255));
            }
            
            return base64_encode($str);
        }
    }
}
?>
