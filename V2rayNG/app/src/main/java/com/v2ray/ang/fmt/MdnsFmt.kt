package com.v2ray.ang.fmt

import android.text.TextUtils
import com.v2ray.ang.dto.entities.ProfileItem
import com.v2ray.ang.enums.EConfigType
import com.v2ray.ang.util.Utils

object MdnsFmt : FmtBase() {
    /**
     * Parses an MDNS URI string into a ProfileItem object.
     * Format: mdns://<base64-encoded-JSON-config>#<remarks-url-encoded>
     *
     * @param str the MDNS URI string to parse
     * @return the parsed ProfileItem object, or null if parsing fails
     */
    fun parse(str: String): ProfileItem? {
        try {
            var rawUri = str
            var remarks = "MasterDNSVPN"

            val hashIdx = str.indexOf('#')
            if (hashIdx >= 0) {
                rawUri = str.substring(0, hashIdx)
                val fragment = str.substring(hashIdx + 1)
                if (fragment.isNotEmpty()) {
                    remarks = Utils.decodeURIComponent(fragment)
                }
            }

            val base64Part = rawUri.replace(EConfigType.MDNS.protocolScheme, "")
            val rawJson = Utils.decode(base64Part)
            if (TextUtils.isEmpty(rawJson)) {
                return null
            }

            // Verify it is a valid JSON
            com.google.gson.JsonParser.parseString(rawJson)

            val config = ProfileItem.create(EConfigType.MDNS)
            config.remarks = remarks
            config.server = "127.0.0.1"
            config.serverPort = "10808"
            config.description = "MasterDNSVPN client configuration"
            config.mdnsRawConfig = rawJson

            return config
        } catch (e: Exception) {
            return null
        }
    }

    /**
     * Converts an MDNS ProfileItem object to a URI string.
     *
     * @param config the ProfileItem object to convert
     * @return the converted URI string
     */
    fun toUri(config: ProfileItem): String {
        val rawJson = config.mdnsRawConfig.orEmpty()
        val base64 = Utils.encode(rawJson)
        return "${base64}#${Utils.encodeURIComponent(config.remarks)}"
    }
}
