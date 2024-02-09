/*---------------------------------------------------------------------------*
 * Copyright (c) 2024 Musarubra, LLC - All Rights Reserved.                     *
 *---------------------------------------------------------------------------*/

package com.opendxl.databus.credential;

import java.util.Map;

public interface Credential {

    /**
     * Gets the credential configuration as a {@code Map<String,Object>}.
     * @return the credential configs.
     */
    Map<String, Object> getCredentialConfig();
}
