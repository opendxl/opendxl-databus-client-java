/*---------------------------------------------------------------------------*
 * Copyright (c) 2019 McAfee, LLC - All Rights Reserved.                     *
 *---------------------------------------------------------------------------*/

package com.opendxl.databus.credential;

import java.util.Map;

public interface Credential {
    Map<String, Object> getCredentialConfig();
}
